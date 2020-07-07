/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Stage.ConfigIssue;
import com.streamsets.pipeline.api.Stage.Context;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_00;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_406;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_407;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaAndTable;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean;
import static java.sql.DriverManager.getConnection;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.Iterator;

public class PostgresCDCWalReceiver {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresCDCWalReceiver.class);
  private static final String TABLE_METADATA_TABLE_SCHEMA_CONSTANT = "table_schem";
  private static final String TABLE_METADATA_TABLE_NAME_CONSTANT = "table_name";
  public static final String SELECT_SLOT = "select * from pg_replication_slots where slot_name = ?";

  private final Properties properties;
  private final String uri;
  private final Context context;
  private final String slotName;
  private final DecoderValues outputPlugin;

  private String configuredPlugin;
  private String configuredSlotType;
  private Boolean slotActive;
  private String restartLsn;
  private String confirmedFlushLSN;
  private Connection connection = null;
  private PGReplicationStream stream;
  private List<SchemaAndTable> schemasAndTables;
  PostgresCDCConfigBean configBean;
  private HikariPoolConfigBean hikariConfigBean;

  private volatile LogSequenceNumber nextLSN;
  private final Object sendUpdatesMutex = new Object();
  private SafeScheduledExecutorService heartBeatSender = new SafeScheduledExecutorService(1, "Postgres Heart Beat Sender");

  private final JdbcUtil jdbcUtil;

  public PostgresCDCWalReceiver(
      PostgresCDCConfigBean configBean,
      HikariPoolConfigBean hikariConfigBean,
      Stage.Context context
  ) throws StageException {
    this.jdbcUtil = UtilsProvider.getJdbcUtil();
    this.configBean = configBean;
    this.hikariConfigBean = hikariConfigBean;
    this.context = context;

    /* TODO resolve issue with using internal Jdbc Read only connection - didn't work
     with postgres replication connection - keeping HikariConfigBean for now */
    try {
      this.connection = getConnection(
          hikariConfigBean.getConnectionString(),
          hikariConfigBean.getUsername().get(),
          hikariConfigBean.getPassword().get());
    } catch (SQLException e) {
      throw new StageException(JDBC_00, e.getMessage(), e);
    }

    this.slotName = configBean.slot;
    this.outputPlugin = configBean.decoderValue;
    this.uri = hikariConfigBean.getConnectionString();
    this.configuredPlugin = null;
    this.configuredSlotType = null;
    this.slotActive = false;
    this.restartLsn = null;
    this.confirmedFlushLSN = null;

    this.properties = new Properties();
    PGProperty.USER.set(properties, hikariConfigBean.getUsername().get());
    PGProperty.PASSWORD.set(properties, hikariConfigBean.getPassword().get());
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, configBean.minVersion.getLabel());
    PGProperty.REPLICATION.set(properties, configBean.replicationType);
    PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
  }

  public List<SchemaAndTable> getSchemasAndTables() {
    return schemasAndTables;
  }

  public Optional<List<ConfigIssue>> validateSchemaAndTables() {
    List<ConfigIssue> issues = new ArrayList<>();
    schemasAndTables = new ArrayList<>();
    for (SchemaTableConfigBean tables : getSchemaAndTableConfig()) {
      validateSchemaAndTable(tables).ifPresent(issues::add);
    }
    if (isThereAFilter() && schemasAndTables.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.CDC.name(),
                  "configBean.baseConfigBean.schemaTableConfigs", JdbcErrors.JDBC_66));
    }
    return Optional.ofNullable(issues);
  }

  List<SchemaTableConfigBean> getSchemaAndTableConfig(){
    return configBean.baseConfigBean.schemaTableConfigs;
  }

  private boolean isThereAFilter() {
    //If there is any value configured for inclusion returns True else returns False
    boolean filterExist = false;
    Iterator it = getSchemaAndTableConfig().iterator();
    while (!filterExist && it.hasNext()) {
      SchemaTableConfigBean tables = (SchemaTableConfigBean)it.next();
      filterExist = !(tables.schema.isEmpty() && tables.table.isEmpty());
    }
    return filterExist;
  }

  private Optional<ConfigIssue> validateSchemaAndTable(SchemaTableConfigBean tables) {
    ConfigIssue issue = null;
    // Empty keys match ALL :(
    if (tables.schema.isEmpty() && tables.table.isEmpty()) {
      return Optional.ofNullable(issue);
    }
    Pattern p = StringUtils.isEmpty(tables.excludePattern) ? null : Pattern.compile(tables.excludePattern);
    try (ResultSet rs =
             getJdbcUtil().getTableAndViewMetadata(connection, tables.schema, tables.table)) {
      while (rs.next()) {
        String schemaName = rs.getString(TABLE_METADATA_TABLE_SCHEMA_CONSTANT);
        String tableName = rs.getString(TABLE_METADATA_TABLE_NAME_CONSTANT);
        if (p == null || !p.matcher(tableName).matches()) {
          schemaName = schemaName.trim();
          tableName = tableName.trim();
          // Passed validation, added
          schemasAndTables.add(new SchemaAndTable(schemaName, tableName));
        }
      }
    } catch (SQLException e) {
      issue = getContext().createConfigIssue(Groups.CDC.name(), tables.schema, JdbcErrors.JDBC_66);
    }
    return Optional.ofNullable(issue);
  }

  public void createReplicationSlot(String slotName) throws StageException {
    try (PreparedStatement preparedStatement =
             connection.prepareStatement("SELECT * FROM pg_create_logical_replication_slot(?, ?)")) {
      preparedStatement.setString(1, slotName);
      preparedStatement.setString(2, outputPlugin.getLabel());
      try (ResultSet rs = preparedStatement.executeQuery()) {
        while (rs.next()) {
          if ( ! slotName.equals(rs.getString(1))) {
            throw new StageException(JDBC_407);
          }
          LOG.debug("Slot Name: " +  rs.getString(1) + " " + rs.getString(2));
        }
      }
    } catch (SQLException e) {
      throw new StageException(JDBC_00, e.getMessage(), e);
    }
  }

  public LogSequenceNumber createReplicationStream(String startOffset)
      throws StageException, InterruptedException, TimeoutException, SQLException {

    boolean newSlot = false;
    if (!doesReplicationSlotExists(slotName)) {
      createReplicationSlot(slotName);
      newSlot = true;
    }
    obtainReplicationSlotInfo(slotName);

    connection = getConnection(this.uri, this.properties);
    PGConnection pgConnection = connection.unwrap(PGConnection.class);

    ChainedLogicalStreamBuilder streamBuilder = pgConnection
        .getReplicationAPI()
        .replicationStream()
        .logical()
        .withSlotName(slotName)
        .withSlotOption("include-xids", true)
        .withSlotOption("include-timestamp", true)
        .withSlotOption("include-lsn", true);

    LogSequenceNumber streamLsn;
    LogSequenceNumber serverFlushedLsn = LogSequenceNumber.valueOf(confirmedFlushLSN);
    if (newSlot) {
      //if the replication slot was just created setting the start offset to an older LSN is a NO OP
      //setting it to a future LSN is risky as the LSN could be invalid (we have to consider the LSN an opaque value).
      //we set the offset then to the 'confirmed_flush_lsn' of the replication slot, that happens to be the
      //the starting point of the newly created replication slot.
      //
      //NOTE that the DATE filter, if a date in the future, it will work as expected because we filter  by DATE.
      streamLsn = serverFlushedLsn;
    } else {

      switch (configBean.startValue) {
        case LATEST:
          // we pick up what is in the replication slot
          streamLsn = serverFlushedLsn;
          break;
        case LSN:
        case DATE:
          LogSequenceNumber configStartLsn = LogSequenceNumber.valueOf(startOffset);
          if (configStartLsn.asLong() > serverFlushedLsn.asLong()) {
            // the given LSN is newer than the last flush, we can safely forward the stream to it,
            // referenced data (by the given configStartLsn should be there)
            streamLsn = configStartLsn;
          } else {
            // we ignore the config start LSN as it is before the last flushed, not in the server anymore
            // this is the normal scenario on later pipeline restarts
            streamLsn = serverFlushedLsn;
            LOG.debug(
                "Configuration Start LSN '{}' is older than server Flushed LSN '{}', this is expected after the first pipeline run",
                configStartLsn,
                serverFlushedLsn
            );
          }
          break;
        default:
          throw new IllegalStateException("Should not happen startValue enum not handled" + configBean.startValue);
      }
    }
    streamBuilder.withStartPosition(streamLsn);

    stream = streamBuilder.start();

    LOG.debug("Starting the Stream with LSN : {}", streamLsn);

    heartBeatSender.scheduleAtFixedRate(this::sendUpdates, 1, 900, TimeUnit.MILLISECONDS);
    return streamLsn;
  }

  private LogSequenceNumber getLogSequenceNumber(String startOffset) {
    LogSequenceNumber lsn = null;

    switch(configBean.startValue) {

      case LATEST:
        //startOffset is always NULL when LATEST per PostgresCDCSource.validatePostgresCDCConfigBean()
        if (startOffset == null) {
          startOffset = confirmedFlushLSN;
        }
        LogSequenceNumber lsnStartOffset = LogSequenceNumber.valueOf(startOffset);
        LogSequenceNumber lsnConfirmedFlush = LogSequenceNumber.valueOf(confirmedFlushLSN);
        lsn = lsnStartOffset.asLong() > lsnConfirmedFlush.asLong() ?
            lsnStartOffset : lsnConfirmedFlush;
        break;

      case LSN:
        //startOffset is config.lsn
      case DATE:
        //startOffset is always 1L (which it is earliest avail)

        // is never NULL here
        if (startOffset == null) {
          startOffset = PostgresCDCSource.SEED_LSN;
        }
        lsn = LogSequenceNumber.valueOf(startOffset);
        break;

      default:
        //should throw exception
    }
    return lsn; //never NULL
  }

  public void dropReplicationSlot(String slotName)
      throws StageException
  {
    try (Connection localConnection = DriverManager.getConnection(
        hikariConfigBean.getConnectionString(),
        hikariConfigBean.getUsername().get(),
        hikariConfigBean.getPassword().get()
    )) {
      if (isReplicationSlotActive(slotName)) {
        try (PreparedStatement preparedStatement = localConnection.prepareStatement(
            "select pg_terminate_backend(active_pid) from pg_replication_slots "
                + "where active = true and slot_name = ?")) {
          preparedStatement.setString(1, slotName);
          preparedStatement.execute();
        }
        waitStopReplicationSlot(slotName);
      }

      try (PreparedStatement preparedStatement = localConnection
          .prepareStatement("select pg_drop_replication_slot(slot_name) "
              + "from pg_replication_slots where slot_name = ?")) {
        preparedStatement.setString(1, slotName);
        preparedStatement.execute();
      }
    } catch (SQLException e) {
      throw new StageException(JDBC_407, slotName, e);
    }
  }

  public void obtainReplicationSlotInfo(String slotName) throws StageException {
    try {
      try (Connection localConnection = DriverManager.getConnection(
          hikariConfigBean.getConnectionString(),
          hikariConfigBean.getUsername().get(),
          hikariConfigBean.getPassword().get()
      )) {
        String sql = SELECT_SLOT;
        String flushedLabel = "confirmed_flush_lsn";
        boolean hasFlushLsn = false;
        try (PreparedStatement preparedStatement = localConnection
            .prepareStatement(sql)) {
          preparedStatement.setString(1, slotName);
          try (ResultSet rs = preparedStatement.executeQuery()) {

            ResultSetMetaData rsmd = rs.getMetaData();
            int columns = rsmd.getColumnCount();
            for (int x = 1; x <= columns; x++) {
              if (flushedLabel.equals(rsmd.getColumnName(x))) {
                hasFlushLsn=true;
                break;
              }
            }
            if (!hasFlushLsn) {
              LOG.debug("No column: confirmed_flush_lsn found. Using restart_lsn");
              flushedLabel="restart_lsn";
            }

            while (rs.next()) {
              this.configuredPlugin = rs.getString("plugin");
              this.configuredSlotType = rs.getString("slot_type");
              this.slotActive = rs.getBoolean("active");
              this.restartLsn = rs.getString("restart_lsn");
              this.confirmedFlushLSN = rs.getString(flushedLabel);
              LOG.debug("Restart LSN - {}, Confirmed Flush LSN - {}", restartLsn, confirmedFlushLSN);
            }
          }
        }
      }

    } catch (SQLException e) {
      throw new StageException(JDBC_407, slotName, e);
    }
  }

  public boolean isReplicationSlotActive(String slotName)
      throws StageException
  {
    obtainReplicationSlotInfo(slotName);
    return slotActive;
  }

  public boolean doesReplicationSlotExists(String slotName) throws
      StageException {
    obtainReplicationSlotInfo(slotName);
    // if replication slot does no exist we don't have a configured plugin
    return configuredPlugin != null;
  }

  private void waitStopReplicationSlot(String slotName)
      throws StageException
  {
    long startWaitTime = System.currentTimeMillis();
    boolean stillActive;
    long timeInWait = 0;

    do {
      stillActive = isReplicationSlotActive(slotName);
      if (stillActive) {
        ThreadUtil.sleep(100L);
        timeInWait = System.currentTimeMillis() - startWaitTime;
      }
    } while (stillActive && timeInWait <= 30000);

    if (stillActive) {
      throw new StageException(JDBC_406, slotName);
    }
  }

  public void commitCurrentOffset() throws StageException {
    synchronized (sendUpdatesMutex) {
      LogSequenceNumber lsn = getNextLSN();
      if (lsn != null) {
        LOG.debug("Flushing LSN END: {}", lsn.asString());
        stream.setAppliedLSN(lsn);
        stream.setFlushedLSN(lsn);
        sendUpdates();
      }
    }
  }

  private void sendUpdates() {
    synchronized (sendUpdatesMutex) {
      try {
        LOG.debug("Sending status updates");
        stream.forceUpdateStatus();
      } catch (SQLException e) {
        // Heart beat sender thread is not currently propagating to main thread
        // Even without that, if the main thread read will fail if there
        // are connectivity issues.
        LOG.error("Error forcing update status: {}", e.getMessage());
        throw new StageException(JDBC_00, " forceUpdateStatus failed :" + e.getMessage(), e);
      }
    }
  }

  public void closeConnection() throws SQLException {
    heartBeatSender.shutdown();
    try {
      //Awaiting 10 seconds only as frequency is ~1 seconds
      heartBeatSender.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted the await of heart beat sender shutdown");
      Thread.currentThread().interrupt();
    }
    if (connection != null) {
      connection.close();
    }
  }

  private ByteBuffer readNonBlocking() throws SQLException {
    return stream.readPending();
  }

  public LogSequenceNumber getCurrentLSN() {
    return stream.getLastReceiveLSN();
  }

  public LogSequenceNumber getNextLSN() {
    return nextLSN;
  }

  public void setNextLSN(LogSequenceNumber nextLSN) {
    this.nextLSN = nextLSN;
  }

  public PostgresWalRecord read() {
    PostgresWalRecord ret = null;
    try {
      ByteBuffer buffer = readNonBlocking();
      if(buffer != null) {
        ret = new PostgresWalRecord(
            buffer,
            getCurrentLSN(),
            configBean.decoderValue
        );
        //sets next LSN
        setNextLSN(LogSequenceNumber.valueOf(ret.getNextLSN()));
      } else {
        LOG.debug("Buffer null");
      }

    } catch (SQLException e) {
      LOG.error(
          Utils.format(
              "Error reading PostgreSQL replication stream: {}",
              e.getMessage()
          ),
          e
      );
      throw new StageException(JdbcErrors.JDBC_407, e);
    }
    return ret;
  }

  public JdbcUtil getJdbcUtil() {
    return jdbcUtil;
  }

    public Context getContext() {
        return context;
    }
}
