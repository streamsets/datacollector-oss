/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.network.ServerException;
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.jdbc.connection.MySQLConnection;
import com.streamsets.pipeline.stage.origin.mysql.filters.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.streamsets.pipeline.stage.origin.mysql.MysqlDSource.CONFIG_PREFIX;
import static com.streamsets.pipeline.stage.origin.mysql.MysqlDSource.CONNECTION_PREFIX;

public abstract class MysqlSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(MysqlSource.class);

  private DataSourceInitializer dataSourceInitializer;

  private BinaryLogConsumer consumer;

  private BinaryLogClient client;

  private EventBuffer eventBuffer;

  private boolean checkBatchSize = true;

  private final BlockingQueue<ServerException> serverErrors = new LinkedBlockingQueue<>();

  private final RecordConverter recordConverter = new RecordConverter(new RecordFactory() {
    @Override
    public Record create(String recordSourceId) {
      return getContext().createRecord(recordSourceId);
    }
  });

  public abstract MySQLBinLogConfig getConfig();
  public abstract MySQLConnection getConnection();

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    dataSourceInitializer = new DataSourceInitializer(
        CONNECTION_PREFIX,
        getConnection(),
        CONFIG_PREFIX,
        getConfig(),
        new ConfigIssueFactory(getContext())
    );

    issues.addAll(dataSourceInitializer.issues);

    return issues;
  }

  @Override
  public void destroy() {
    if (client != null) {
      try {
        client.disconnect();
      } catch (IOException e) {
        LOG.warn("Error disconnecting from MySql", e);
      }
    }

    if (dataSourceInitializer != null) {
      dataSourceInitializer.destroy();
    }

    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    // client does not get connected in init(), instead it is connected on first
    // invocation of this produce(), as we need to advance it to specific offset
    if (client == null) {
      // connect consumer handling all events to client
      MysqlSchemaRepository schemaRepository = new MysqlSchemaRepository(dataSourceInitializer.dataSource);
      eventBuffer = new EventBuffer(getConfig().maxBatchSize);
      client = dataSourceInitializer.createBinaryLogClient();
      consumer = new BinaryLogConsumer(schemaRepository, eventBuffer, client);

      connectClient(client, lastSourceOffset);
      LOG.info("Connected client with configuration: {}", getConfig());
    }

    // in case of empty batch we don't want to stop
    if (lastSourceOffset == null) {
      lastSourceOffset = "";
    }

    LOG.trace("Client position at the begging of a batch {}:{}", client.getBinlogFilename(), client.getBinlogPosition());
    LOG.trace("Stored offset at the begging of a batch: {}", lastSourceOffset);

    // since last invocation there could errors happen
    handleErrors();

    int recordCounter = 0;
    int batchSize = getConfig().maxBatchSize > maxBatchSize ? maxBatchSize : getConfig().maxBatchSize;
    if (!getContext().isPreview() && checkBatchSize && getConfig().maxBatchSize > maxBatchSize) {
      getContext().reportError(Errors.MYSQL_010, maxBatchSize);
      checkBatchSize = false;
    }

    long startTime = System.currentTimeMillis();
    while (recordCounter < batchSize && (startTime + getConfig().maxWaitTime) > System.currentTimeMillis()) {
      long timeLeft = getConfig().maxWaitTime - (System.currentTimeMillis() - startTime);
      if (timeLeft < 0) {
        break;
      }
      EnrichedEvent event = eventBuffer.poll(timeLeft, TimeUnit.MILLISECONDS);
      // check errors
      handleErrors();

      if (event != null) {
        // move offset no matter if event is filtered out
        lastSourceOffset = event.getOffset().format();

        // check if event should be filtered out
        if (dataSourceInitializer.eventFilter.apply(event) == Filter.Result.PASS) {
          List<Record> records = recordConverter.toRecords(event);
          // If we are in preview mode, make sure we don't send a huge number of messages.
          if (getContext().isPreview() && recordCounter + records.size() > batchSize) {
            records = records.subList(0, batchSize - recordCounter);
          }
          for (Record record : records) {
            batchMaker.addRecord(record);
          }
          recordCounter += records.size();
        } else {
          LOG.trace(
              "Event for {}.{} filtered out",
              event.getTable().getDatabase(),
              event.getTable().getName()
          );
        }
      }
    }

    LOG.trace("Client position at the end of a batch {}:{}", client.getBinlogFilename(), client.getBinlogPosition());
    LOG.trace("Stored offset at the end of a batch: {}", lastSourceOffset);
    return lastSourceOffset;
  }

  private void connectClient(BinaryLogClient client, String lastSourceOffset) throws StageException {
    try {
      if (lastSourceOffset == null) {
        // first start
        if (getConfig().initialOffset != null && !getConfig().initialOffset.isEmpty()) {
          // start from config offset
          SourceOffset offset = dataSourceInitializer.offsetFactory.create(getConfig().initialOffset);
          LOG.info("Moving client to offset {}", offset);
          offset.positionClient(client);
          consumer.setOffset(offset);
        } else if (getConfig().startFromBeginning) {
          if (isGtidEnabled()) {
            // when starting from beginning with GTID - skip GTIDs that have been removed
            // from server logs already
            GtidSet purged = new GtidSet(Util.getServerGtidPurged(dataSourceInitializer.dataSource));
            // client's gtidset includes first event of purged, skip latest tx of purged
            for (GtidSet.UUIDSet uuidSet : purged.getUUIDSets()) {
              GtidSet.Interval last = null;
              for (GtidSet.Interval interval : uuidSet.getIntervals()) {
                last = interval;
              }
              purged.add(String.format("%s:%s", uuidSet.getUUID(), last.getEnd()));
            }
            LOG.info("Skipping purged gtidset {}", purged);
            client.setGtidSet(purged.toString());
          } else {
            // gtid_mode = off
            client.setBinlogFilename("");
          }
        } else {
          // read from current position
          if (isGtidEnabled()) {
            // set client gtidset to master executed gtidset
            String executed = Util.getServerGtidExecuted(dataSourceInitializer.dataSource);
            // if position client to 'executed' - it will fetch last transaction
            // so - advance client to +1 transaction
            String serverUUID = Util.getGlobalVariable(dataSourceInitializer.dataSource, "server_uuid");
            GtidSet ex = new GtidSet(executed);
            for (GtidSet.UUIDSet uuidSet : ex.getUUIDSets()) {
              if (uuidSet.getUUID().equals(serverUUID)) {
                List<GtidSet.Interval> intervals = new ArrayList<>(uuidSet.getIntervals());
                GtidSet.Interval last = intervals.get(intervals.size() - 1);
                ex.add(String.format("%s:%d", serverUUID, last.getEnd()));
                break;
              }
            }
            client.setGtidSet(ex.toString());
          }
        }
      } else {
        // resume work from previous position
        if (!"".equals(lastSourceOffset)) {
          SourceOffset offset = dataSourceInitializer.offsetFactory.create(lastSourceOffset);
          LOG.info("Moving client to offset {}", offset);
          offset.positionClient(client);
          consumer.setOffset(offset);
        }
      }

      client.setKeepAlive(getConfig().enableKeepAlive);
      if (getConfig().enableKeepAlive) {
        client.setKeepAliveInterval(getConfig().keepAliveInterval);
      }
      registerClientLifecycleListener();
      client.registerEventListener(consumer);
      client.connect(getConfig().connectTimeout);
    } catch (IOException | TimeoutException | SQLException e) {
      LOG.error(Errors.MYSQL_003.getMessage(), e.toString(), e);
      throw new StageException(Errors.MYSQL_003, e.toString(), e);
    }
  }

  private void registerClientLifecycleListener() {
    client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
      @Override
      public void onConnect(BinaryLogClient client) {
        LOG.info("Connected to server {} with client position {}:{}",
          client.getMasterServerId(),
          client.getBinlogFilename(),
          client.getBinlogPosition()
        );
      }

      @Override
      public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
        if (ex instanceof ServerException) {
          serverErrors.add((ServerException) ex);
        } else {
          LOG.error("Unhandled communication error with client at position {}:{}: {}",
              ex.getMessage(),
              client.getBinlogFilename(),
              client.getBinlogFilename(),
              ex
          );
        }
      }

      @Override
      public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
        LOG.error("Error deserializing event with client at position {}:{}: {}",
            ex.getMessage(),
            client.getBinlogFilename(),
            client.getBinlogPosition(),
            ex
        );
      }

      @Override
      public void onDisconnect(BinaryLogClient client) {
        LOG.info("Disconnected from server {} with client at position {}:{}",
            client.getMasterServerId(),
            client.getBinlogFilename(),
            client.getBinlogPosition()
        );
      }
    });
  }

  private boolean isGtidEnabled() {
    try {
      return "ON".equals(Util.getGlobalVariable(dataSourceInitializer.dataSource, "gtid_mode"));
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
  }

  private void handleErrors() throws StageException {
    for (ServerException e : serverErrors) {
      LOG.error("BinaryLogClient server error: {}", e.getMessage(), e);
    }
    ServerException e = serverErrors.poll();
    if (e != null) {
      // Dump current position of the client
      LOG.debug("Client position while handling error: {}:{}", client.getBinlogFilename(), client.getBinlogPosition());
      // Dump additional database state to help troubleshoot what is going on
      dumpServerReplicationStatus();
      // record policy does not matter - stop pipeline
      throw new StageException(Errors.MYSQL_006, e.getMessage(), e);
    }
  }

  /**
   * Dump various replication status from the server into log, to aim with debugging in case that there is something
   * weird going on.
   */
  private void dumpServerReplicationStatus() {
    dumpQueryToLogs("show master status");
    dumpQueryToLogs("show slave status");
    dumpQueryToLogs("show binary logs");
  }

  /**
   * Execute given query and print it into logs. We expect that the result set is small and will be cut on 20 entries.
   *
   * @param query
   */
  private void dumpQueryToLogs(String query) {
    try (
      Connection connection = dataSourceInitializer.dataSource.getConnection();
      Statement statement = connection.createStatement();
      ResultSet rs = statement.executeQuery(query)
    ) {
      StringBuilder builder = new StringBuilder();
      ResultSetMetaData metadata = rs.getMetaData();

      LOG.debug("Output of query: {}", query);
      for(int i = 1; i <= metadata.getColumnCount(); i++) {
        builder.append(metadata.getColumnName(i));
        builder.append(';');
      }
      LOG.debug(builder.toString());

      int rows = 0;
      while(rs.next()) {
        rows++;

        builder = new StringBuilder();
        for(int i = 1; i <= metadata.getColumnCount(); i++) {
          builder.append(rs.getObject(i).toString());
          builder.append(';');
        }
        LOG.debug(builder.toString());
      }
      LOG.debug("{} rows", rows);
    } catch (Throwable e) {
      LOG.error("Got error while executing: {}", query, e);
    }
  }
}
