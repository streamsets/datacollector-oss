/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.mysql.binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;
import com.github.shyiko.mysql.binlog.network.ServerException;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.streamsets.datacollector.security.KeyStoreBuilder;
import com.streamsets.datacollector.security.KeyStoreIO;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import com.streamsets.pipeline.lib.jdbc.BasicConnectionString;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.stage.config.MySQLBinLogGroups;
import com.streamsets.pipeline.stage.config.MysqlBinLogSourceConfig;
import com.streamsets.pipeline.stage.origin.event.EnrichedEvent;
import com.streamsets.pipeline.stage.origin.event.EventBuffer;
import com.streamsets.pipeline.stage.origin.mysql.MysqlSchemaRepository;
import com.streamsets.pipeline.stage.origin.mysql.RecordConverter;
import com.streamsets.pipeline.stage.origin.mysql.Util;
import com.streamsets.pipeline.stage.origin.mysql.error.MySQLBinLogErrors;
import com.streamsets.pipeline.stage.origin.mysql.filters.Filter;
import com.streamsets.pipeline.stage.origin.mysql.filters.Filters;
import com.streamsets.pipeline.stage.origin.mysql.filters.IgnoreTableFilter;
import com.streamsets.pipeline.stage.origin.mysql.filters.IncludeTableFilter;
import com.streamsets.pipeline.stage.origin.mysql.offset.BinLogPositionOffsetFactory;
import com.streamsets.pipeline.stage.origin.mysql.offset.BinLogPositionSourceOffset;
import com.streamsets.pipeline.stage.origin.mysql.offset.GtidSourceOffsetFactory;
import com.streamsets.pipeline.stage.origin.mysql.offset.SourceOffset;
import com.streamsets.pipeline.stage.origin.mysql.offset.SourceOffsetFactory;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class MysqlBinLogSource extends BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlBinLogSource.class);

  private static final String VAR_SERVER_UUID = "server_uuid";
  private static final String VAR_GTID_MODE = "gtid_mode";
  private static final String VAR_GTID_MODE_ON = "ON";

  private BinaryLogConsumer consumer;
  private BinaryLogClient client;
  private EventBuffer eventBuffer;
  private HikariDataSource dataSource;
  private SourceOffsetFactory offsetFactory;
  private Filter eventFilter;
  private SshTunnelService sshTunnelService;
  private SshTunnelService.HostPort sshTunnel;
  private JdbcUtil jdbcUtil = new JdbcUtil();
  private boolean checkBatchSize = true;

  private MysqlBinLogSourceConfig config;

  private final BlockingQueue<ServerException> serverErrors = new LinkedBlockingQueue<>();

  private final RecordConverter recordConverter = new RecordConverter(recordSourceId -> getContext().createRecord(
      recordSourceId));

  public abstract MysqlBinLogSourceConfig getConfig();

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    Source.Context context = getContext();

    config = getConfig();

    sshTunnelService = context.getService(SshTunnelService.class);

    BasicConnectionString.Info info = config.getBasicConnectionString()
                                            .getBasicConnectionInfo(config.buildConnectionString());

    if (info != null) {
      // basic connection string format
      SshTunnelService.HostPort target = new SshTunnelService.HostPort(info.getHost(), info.getPort());
      Map<SshTunnelService.HostPort, SshTunnelService.HostPort>
          portMapping
          = sshTunnelService.start(Collections.singletonList(target));
      this.sshTunnel = portMapping.get(target);
      info = info.changeHostPort(sshTunnel.getHost(), sshTunnel.getPort());
      config.setConnectionString(config.getBasicConnectionString().getBasicConnectionUrl(info));
    } else {
      // complex connection string format, we don't support this right now with SSH tunneling
      issues.add(getContext().createConfigIssue("JDBC", "hikariConfigBean.sslMode", config.getNonBasicUrlErrorCode()));
    }


    if (issues.isEmpty()) {
      config.validateConfigs(context, issues);
    }

    // connect to mysql
    try {
      dataSource = jdbcUtil.createDataSourceForRead(config);
      offsetFactory = isGtidEnabled() ? new GtidSourceOffsetFactory() : new BinLogPositionOffsetFactory();
    } catch (HikariPool.PoolInitializationException e) {
      LOG.error(MySQLBinLogErrors.MYSQL_BIN_LOG_003.getMessage(), e.getMessage(), e);
      issues.add(context.createConfigIssue(MySQLBinLogGroups.JDBC.name(),
          null,
          MySQLBinLogErrors.MYSQL_BIN_LOG_003,
          e.getMessage(),
          e
      ));
    }

    // check if binlog client connection is possible. We don't reuse this client later on, it is used just to check
    // that client can connect, it is immediately closed after connection.
    if (issues.isEmpty()) {
      BinaryLogClient tmpClient = createBinaryLogClient(config);
      try {
        tmpClient.setKeepAlive(false);
        tmpClient.connect(config.connectionTimeout);
      } catch (IOException | TimeoutException e) {
        LOG.error(MySQLBinLogErrors.MYSQL_BIN_LOG_003.getMessage(), e.getMessage(), e);
        issues.add(context.createConfigIssue(MySQLBinLogGroups.JDBC.name(),
            MysqlBinLogSourceConfig.CONFIG_HOSTNAME,
            MySQLBinLogErrors.MYSQL_BIN_LOG_003,
            e.getMessage(),
            e
        ));
      } finally {
        try {
          tmpClient.disconnect();
        } catch (IOException e) {
          LOG.warn(MySQLBinLogErrors.MYSQL_BIN_LOG_009.getMessage(), e.getMessage(), e);
        }
      }
    }

    if (issues.isEmpty() && !config.startFromBeginning) {
      // offset
      try {
        BinLogPositionSourceOffset.parse(config.initialOffset);
      } catch (Exception e) {
        LOG.error(MySQLBinLogErrors.MYSQL_BIN_LOG_011.getMessage(), e.getMessage(), e);
        issues.add(context.createConfigIssue(MySQLBinLogGroups.ADVANCED.name(),
            MysqlBinLogSourceConfig.CONFIG_INITIAL_OFFSET,
            MySQLBinLogErrors.MYSQL_BIN_LOG_011,
            config.initialOffset
        ));
      }
    }

    if (issues.isEmpty()) {
      // create include/ignore filters
      Filter includeFilter = null;
      try {
        includeFilter = createIncludeFilter();
      } catch (IllegalArgumentException e) {
        LOG.error(MySQLBinLogErrors.MYSQL_BIN_LOG_015.getMessage(), e.getMessage(), e);
        issues.add(context.createConfigIssue(MySQLBinLogGroups.ADVANCED.name(),
            MysqlBinLogSourceConfig.CONFIG_INCLUDE_TABLES,
            MySQLBinLogErrors.MYSQL_BIN_LOG_008,
            e.getMessage(),
            e
        ));
      }

      Filter ignoreFilter = null;
      try {
        ignoreFilter = createIgnoreFilter();
      } catch (IllegalArgumentException e) {
        LOG.error(MySQLBinLogErrors.MYSQL_BIN_LOG_016.getMessage(), e.getMessage(), e);
        issues.add(context.createConfigIssue(MySQLBinLogGroups.ADVANCED.name(),
            MysqlBinLogSourceConfig.CONFIG_IGNORE_TABLES,
            MySQLBinLogErrors.MYSQL_BIN_LOG_007,
            e.getMessage(),
            e
        ));
      }

      if (ignoreFilter != null && includeFilter != null) {
        eventFilter = includeFilter.and(ignoreFilter);
      }
    }
    return issues;
  }

  private BinaryLogClient createBinaryLogClient(MysqlBinLogSourceConfig config) {
    BinaryLogClient binLogClient = new BinaryLogClient(
        sshTunnel.getHost(),
        sshTunnel.getPort(),
        config.username.get(),
        config.password.get()
    );

    switch (config.sslMode) {
      case REQUIRED:
        binLogClient.setSSLMode(SSLMode.REQUIRED);
        break;
      case VERIFY_CA:
        binLogClient.setSSLMode(SSLMode.VERIFY_CA);
        setSSLSocketFactory(binLogClient);
        break;
      case VERIFY_IDENTITY:
        binLogClient.setSSLMode(SSLMode.VERIFY_IDENTITY);
        setSSLSocketFactory(binLogClient);
        break;
    }

    binLogClient.setServerId(config.serverId);
    return binLogClient;
  }

  @Override
  public void destroy() {
    if (client != null && client.isConnected()) {
      try {
        client.disconnect();
      } catch (IOException e) {
        LOG.warn(MySQLBinLogErrors.MYSQL_BIN_LOG_017.getMessage(), e);
      }
    }

    if (sshTunnelService != null) {
      sshTunnelService.stop();
    }

    if (dataSource != null) {
      dataSource.close();
    }

    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    sshTunnelService.healthCheck();
    // connect client & advance it to specific offset
    if (client == null) {
      MysqlSchemaRepository schemaRepository = new MysqlSchemaRepository(dataSource);
      eventBuffer = new EventBuffer(config.maxBatchSize);
      client = createBinaryLogClient(config);
      consumer = new BinaryLogConsumer(schemaRepository, eventBuffer, client);

      connectClient(client, lastSourceOffset);
      LOG.info("Connected client with configuration: {}", config);
    }

    // in case of empty batch we don't want to stop
    if (lastSourceOffset == null) {
      lastSourceOffset = "";
    }

    // since last invocation there could errors happen
    handleErrors();

    int recordCounter = 0;
    int batchSize = config.maxBatchSize > maxBatchSize ? maxBatchSize : config.maxBatchSize;
    if (!getContext().isPreview() && checkBatchSize && config.maxBatchSize > maxBatchSize) {
      getContext().reportError(MySQLBinLogErrors.MYSQL_BIN_LOG_018, maxBatchSize);
      checkBatchSize = false;
    }

    long startTime = System.currentTimeMillis();

    while (recordCounter < batchSize && (startTime + config.maxWaitTime) > System.currentTimeMillis()) {
      long timeLeft = config.maxWaitTime - (System.currentTimeMillis() - startTime);

      if (timeLeft < 0) {
        break;
      }
      EnrichedEvent event = eventBuffer.poll(timeLeft, TimeUnit.MILLISECONDS, getContext().isPreview());
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
          LOG.trace("Event for {}.{} filtered out", event.getTable().getDatabase(), event.getTable().getName());
        }
      }
    }
    return lastSourceOffset;
  }

  private void connectClient(BinaryLogClient client, String lastSourceOffset) throws StageException {
    try {
      if (lastSourceOffset == null) {

        if (config.startFromBeginning) {
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
        } else if (config.initialOffset != null && !config.initialOffset.isEmpty()) {
          // start from config offset
          SourceOffset offset = offsetFactory.create(config.initialOffset);
          LOG.info("Moving client to offset {}", offset);
          offset.positionClient(client);
          consumer.setOffset(offset);
        } else {
          // read from current position
          if (isGtidEnabled()) {
            // set client gtidset to master executed gtidset
            String executed = Util.getServerGtidExecuted(dataSource);
            // if position client to 'executed' - it will fetch last transaction
            // so - advance client to +1 transaction
            String serverUUID = Util.getGlobalVariable(dataSource, VAR_SERVER_UUID);
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

      client.setKeepAlive(config.enableKeepAlive);
      if (config.enableKeepAlive) {
        client.setKeepAliveInterval(config.keepAliveInterval);
      }
      registerClientLifecycleListener();
      client.registerEventListener(consumer);
      client.connect(config.connectionTimeout);
    } catch (IOException | TimeoutException | SQLException e) {
      LOG.error(MySQLBinLogErrors.MYSQL_BIN_LOG_003.getMessage(), e.toString(), e);
      throw new StageException(MySQLBinLogErrors.MYSQL_BIN_LOG_003, e.toString(), e);
    }
  }

  private Filter createIgnoreFilter() {
    Filter filter = Filters.PASS;
    if (config.ignoreTables != null && !config.ignoreTables.isEmpty()) {
      for (String table : config.ignoreTables.split(",")) {
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
    if (config.includeTables != null && !config.includeTables.isEmpty()) {
      String[] includeTables = config.includeTables.split(",");
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
          LOG.error(MySQLBinLogErrors.MYSQL_BIN_LOG_012.getMessage(), ex.getMessage(), ex);
        }
      }

      @Override
      public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
        LOG.error(MySQLBinLogErrors.MYSQL_BIN_LOG_013.getMessage(), ex.getMessage(), ex);
      }
    });
  }

  private boolean isGtidEnabled() {
    try {
      return VAR_GTID_MODE_ON.equals(Util.getGlobalVariable(dataSource, VAR_GTID_MODE));
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
  }

  private void handleErrors() throws StageException {
    for (ServerException e : serverErrors) {
      LOG.error(MySQLBinLogErrors.MYSQL_BIN_LOG_014.getMessage(), e.getMessage(), e);
    }
    ServerException e = serverErrors.poll();
    if (e != null) {
      // record policy does not matter - stop pipeline
      throw new StageException(MySQLBinLogErrors.MYSQL_BIN_LOG_006, e.getMessage(), e);
    }
  }

  /**
   * Private class to create and set the SSLSocketFactory in the BinaryLogClient
   *
   * @param client the client where we are going to set the SSLSocketFactory
   */
  private void setSSLSocketFactory(BinaryLogClient client) {
    client.setSslSocketFactory(new SSLSocketFactory() {
      private static final String X509 = "SunX509";
      private static final String TLS_DEFAULT = "TLSv1";
      private static final String TLS_QUERY = "SHOW GLOBAL VARIABLES LIKE 'tls_version';";

      @Override
      public SSLSocket createSocket(Socket socket) throws SocketException {
        SSLContext sslContext;

        try {
          sslContext = SSLContext.getInstance(getTLSVersion());

          KeyStore keyStore = new KeyStoreBuilder().addCertificatePem(config.certificatePem).build();
          sslContext.init(getKeyManagers(keyStore), getTrustManagers(keyStore), null);

        } catch (GeneralSecurityException e) {
          throw new SocketException(e.getMessage());
        }
        try {
          return (SSLSocket) sslContext.getSocketFactory().createSocket(socket,
              sshTunnel.getHost(),
              sshTunnel.getPort(),
              true
          );
        } catch (IOException e) {
          throw new SocketException(e.getMessage());
        }
      }

      private KeyManager[] getKeyManagers(KeyStore keyStore) throws GeneralSecurityException {
        KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(X509);
        keyMgrFactory.init(keyStore, KeyStoreIO.save(keyStore).getPassword().toCharArray());
        return keyMgrFactory.getKeyManagers();
      }

      private TrustManager[] getTrustManagers(KeyStore keystore) throws GeneralSecurityException {
        TrustManager[] trustManagers = new TrustManager[1];
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(X509);
        trustManagerFactory.init(keystore);
        for (TrustManager trustManager1 : trustManagerFactory.getTrustManagers()) {
          if (trustManager1 instanceof X509TrustManager) {
            trustManagers[0] = trustManager1;
            break;
          }
        }
        return trustManagers;
      }

      private String getTLSVersion() {
        String tlsVersion = TLS_DEFAULT;
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement(); ResultSet rs = statement
            .executeQuery(TLS_QUERY)) {
          while (rs.next()) {
            String versionsResult = rs.getString(2);
            String databaseVersions = StringUtils.substringAfterLast(versionsResult, ",");
            if (!Strings.isNullOrEmpty(databaseVersions)) {
              tlsVersion = databaseVersions;
            }
            break;
          }
        } catch (SQLException e) {
          LOG.warn("An error ocurred while trying to retrieve TLS version, will use default {}", TLS_DEFAULT);
        }
        return tlsVersion;
      }
    });
  }
}
