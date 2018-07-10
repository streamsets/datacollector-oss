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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.shyiko.mysql.binlog.network.ServerException;
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.stage.origin.mysql.filters.Filter;
import com.streamsets.pipeline.stage.origin.mysql.filters.Filters;
import com.streamsets.pipeline.stage.origin.mysql.filters.IgnoreTableFilter;
import com.streamsets.pipeline.stage.origin.mysql.filters.IncludeTableFilter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.PoolInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MysqlSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(MysqlSource.class);

  private static final String CONFIG_PREFIX = "config.";
  private BinaryLogConsumer consumer;

  private BinaryLogClient client;

  private EventBuffer eventBuffer;

  private HikariDataSource dataSource;

  private SourceOffsetFactory offsetFactory;

  private Filter eventFilter;

  private int port;
  private long serverId;

  private final BlockingQueue<ServerException> serverErrors = new LinkedBlockingQueue<>();

  private final RecordConverter recordConverter = new RecordConverter(new RecordFactory() {
    @Override
    public Record create(String recordSourceId) {
      return getContext().createRecord(recordSourceId);
    }
  });

  public abstract MysqlSourceConfig getConfig();

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    // Validate the port number
    try {
      port = Integer.valueOf(getConfig().port);
    } catch (NumberFormatException e) {
      throw new NumberFormatException("Port number must be numeric");
    }
    // ServerId can be empty. Validate if provided.
    try {
      if (getConfig().serverId != null && !getConfig().serverId.isEmpty())
        serverId = Integer.valueOf(getConfig().serverId);
    } catch (NumberFormatException e) {
      throw new NumberFormatException("Server ID must be numeric");
    }
    // check if binlog client connection is possible
    // we don't reuse this client later on, it is used just to check that client can connect, it
    // is immediately closed after connection.
    BinaryLogClient tmpClient = createBinaryLogClient();
    try {
      tmpClient.setKeepAlive(false);
      tmpClient.connect(getConfig().connectTimeout);
    } catch (IOException | TimeoutException e) {
      LOG.error("Error connecting to MySql binlog: {}", e.getMessage(), e);
      issues.add(getContext().createConfigIssue(
          Groups.MYSQL.name(), CONFIG_PREFIX + "hostname", Errors.MYSQL_003, e.getMessage(), e
      ));
    } finally {
      try {
        tmpClient.disconnect();
      } catch (IOException e) {
        LOG.warn("Error disconnecting from MySql: {}", e.getMessage(), e);
      }
    }

    // create include/ignore filters
    Filter includeFilter = null;
    try {
      includeFilter = createIncludeFilter();
    } catch (IllegalArgumentException e) {
      LOG.error("Error creating include tables filter: {}", e.getMessage(), e);
      issues.add(getContext().createConfigIssue(
          Groups.ADVANCED.name(), CONFIG_PREFIX  + "includeTables", Errors.MYSQL_008, e.getMessage(), e
      ));
    }

    Filter ignoreFilter = null;
    try {
      ignoreFilter = createIgnoreFilter();
    } catch (IllegalArgumentException e) {
      LOG.error("Error creating ignore tables filter: {}", e.getMessage(), e);
      issues.add(getContext().createConfigIssue(
          Groups.ADVANCED.name(), CONFIG_PREFIX + "ignoreTables", Errors.MYSQL_007, e.getMessage(), e
      ));
    }

    if (ignoreFilter != null && includeFilter != null) {
      eventFilter = includeFilter.and(ignoreFilter);
    }

    // connect to mysql
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(String.format("jdbc:mysql://%s:%d", getConfig().hostname, port));
    hikariConfig.setUsername(getConfig().username);
    hikariConfig.setPassword(getConfig().password);
    hikariConfig.setReadOnly(true);
    hikariConfig.addDataSourceProperty("useSSL", getConfig().useSsl);
    try {
      dataSource = new HikariDataSource(hikariConfig);
      offsetFactory = isGtidEnabled()
          ? new GtidSourceOffsetFactory()
          : new BinLogPositionOffsetFactory();
    } catch (PoolInitializationException e) {
      LOG.error("Error connecting to MySql: {}", e.getMessage(), e);
      issues.add(getContext().createConfigIssue(
          Groups.MYSQL.name(), null, Errors.MYSQL_003, e.getMessage(), e
      ));
    }
    return issues;
  }

  private BinaryLogClient createBinaryLogClient() {
    BinaryLogClient binLogClient = new BinaryLogClient(
        getConfig().hostname,
        port,
        getConfig().username,
        getConfig().password
    );
    if (getConfig().useSsl) {
      binLogClient.setSSLMode(SSLMode.REQUIRED);
    } else {
      binLogClient.setSSLMode(SSLMode.DISABLED);
    }
    binLogClient.setServerId(serverId);
    return binLogClient;
  }

  @Override
  public void destroy() {
    if (client != null && client.isConnected()) {
      try {
        client.disconnect();
      } catch (IOException e) {
        LOG.warn("Error disconnecting from MySql", e);
      }
    }

    if (dataSource != null) {
      dataSource.close();
    }

    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    // client does not get connected in init(), instead it is connected on first
    // invocation of this produce(), as we need to advance it to specific offset
    if (client == null) {
      // connect consumer handling all events to client
      MysqlSchemaRepository schemaRepository = new MysqlSchemaRepository(dataSource);
      eventBuffer = new EventBuffer(getConfig().maxBatchSize);
      client = createBinaryLogClient();
      consumer = new BinaryLogConsumer(schemaRepository, eventBuffer, client);

      connectClient(client, lastSourceOffset);
      LOG.info("Connected client with configuration: {}", getConfig());
    }

    // in case of empty batch we don't want to stop
    if (lastSourceOffset == null) {
      lastSourceOffset = "";
    }

    // since last invocation there could errors happen
    handleErrors();

    int recordCounter = 0;
    int batchSize = getConfig().maxBatchSize > maxBatchSize ? maxBatchSize : getConfig().maxBatchSize;
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
        if (eventFilter.apply(event) == Filter.Result.PASS) {
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
    return lastSourceOffset;
  }

  private void connectClient(BinaryLogClient client, String lastSourceOffset) throws StageException {
    try {
      if (lastSourceOffset == null) {
        // first start
        if (getConfig().initialOffset != null && !getConfig().initialOffset.isEmpty()) {
          // start from config offset
          SourceOffset offset = offsetFactory.create(getConfig().initialOffset);
          LOG.info("Moving client to offset {}", offset);
          offset.positionClient(client);
          consumer.setOffset(offset);
        } else if (getConfig().startFromBeginning) {
          if (isGtidEnabled()) {
            // when starting from beginning with GTID - skip GTIDs that have been removed
            // from server logs already
            GtidSet purged = new GtidSet(Util.getServerGtidPurged(dataSource));
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
            String executed = Util.getServerGtidExecuted(dataSource);
            // if position client to 'executed' - it will fetch last transaction
            // so - advance client to +1 transaction
            String serverUUID = Util.getGlobalVariable(dataSource, "server_uuid");
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
          SourceOffset offset = offsetFactory.create(lastSourceOffset);
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

  private Filter createIgnoreFilter() {
    Filter filter = Filters.PASS;
    if (getConfig().ignoreTables != null && !getConfig().ignoreTables.isEmpty()) {
      for (String table : getConfig().ignoreTables.split(",")) {
        if (!table.isEmpty()) {
          filter = filter.and(new IgnoreTableFilter(table));
        }
      }
    }
    return filter;
  }

  private Filter createIncludeFilter() {
    // if there are no include filters - pass
    Filter filter = Filters.PASS;
    if (getConfig().includeTables != null && !getConfig().includeTables.isEmpty()) {
      String[] includeTables = getConfig().includeTables.split(",");
      if (includeTables.length > 0) {
        // ignore all that is not explicitly included
        filter = Filters.DISCARD;
        for (String table : includeTables) {
          if (!table.isEmpty()) {
            filter = filter.or(new IncludeTableFilter(table));
          }
        }
      }
    }
    return filter;
  }

  private void registerClientLifecycleListener() {
    client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
      @Override
      public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
        if (ex instanceof ServerException) {
          serverErrors.add((ServerException) ex);
        } else {
          LOG.error("Unhandled communication error: {}", ex.getMessage(), ex);
        }
      }

      @Override
      public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
        LOG.error("Error deserializing event: {}", ex.getMessage(), ex);
      }
    });
  }

  private boolean isGtidEnabled() {
    try {
      return "ON".equals(Util.getGlobalVariable(dataSource, "gtid_mode"));
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
      // record policy does not matter - stop pipeline
      throw new StageException(Errors.MYSQL_006, e.getMessage(), e);
    }
  }
}
