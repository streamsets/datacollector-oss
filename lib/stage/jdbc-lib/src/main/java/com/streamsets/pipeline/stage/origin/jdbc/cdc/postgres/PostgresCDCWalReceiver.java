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

import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_00;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_406;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_407;
import static java.sql.DriverManager.*;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Stage.ConfigIssue;
import com.streamsets.pipeline.api.Stage.Context;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaAndTable;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresCDCWalReceiver {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresCDCWalReceiver.class);
  private static final String TABLE_METADATA_TABLE_SCHEMA_CONSTANT = "table_schem";
  private static final String TABLE_METADATA_TABLE_NAME_CONSTANT = "table_name";

  private final Properties properties;
  private final String uri;
  private final Context context;
  private final String slotName;
  private final DecoderValues outputPlugin;

  private String offset;
  private String configuredPlugin;
  private String configuredSlotType;
  private Boolean slotActive;
  private String restartLsn;
  private Connection connection = null;
  private PGReplicationStream stream;
  private List<SchemaAndTable> schemasAndTables;
  private PostgresCDCConfigBean configBean;
  private HikariPoolConfigBean hikariConfigBean;

  public PGReplicationStream getStream() {
    return stream;
  }

  public String getOffset() {
    return offset;
  }

  public List<SchemaAndTable> getSchemasAndTables() {
    return schemasAndTables;
  }

  //TODO this is a great place for generic validator
  public Optional<List<ConfigIssue>> validateSchemaAndTables(List<SchemaTableConfigBean>
      schemaTableConfigs) {
    List<ConfigIssue> issues = new ArrayList<>();
    for (SchemaTableConfigBean tables : configBean.baseConfigBean.schemaTableConfigs) {
      validateSchemaAndTable(tables).ifPresent(issues::add);
    }
    return Optional.ofNullable(issues);
  }

  private Optional<ConfigIssue> validateSchemaAndTable(SchemaTableConfigBean tables) {
    schemasAndTables = new ArrayList<>();
    List<ConfigIssue> issues = new ArrayList<>();
    Pattern p = StringUtils.isEmpty(tables.excludePattern) ? null : Pattern.compile(tables.excludePattern);
    try (ResultSet rs =
        JdbcUtil.getTableMetadata(connection, null, tables.schema, tables.table, true)) {
      while (rs.next()) {
        String schemaName = rs.getString(TABLE_METADATA_TABLE_SCHEMA_CONSTANT);
        String tableName = rs.getString(TABLE_METADATA_TABLE_NAME_CONSTANT);
        if (p == null || !p.matcher(tableName).matches()) {
          schemaName = schemaName.trim();
          tableName = tableName.trim();
          schemasAndTables.add(new SchemaAndTable(schemaName, tableName));
        }
      }
    } catch (SQLException e) {
      issues.add(
          context.createConfigIssue(
              Groups.CDC.name(),
              tables.schema,
              JdbcErrors.JDBC_66
              )
      );
    }
    return Optional.ofNullable(null);
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
          this.offset = rs.getString(2);
        }
      }
    } catch (SQLException e) {
      throw new StageException(JDBC_00);
    }
  }

  public String createReplicationStream(String startOffset)
      throws StageException, InterruptedException, TimeoutException, SQLException {

    if ( ! isReplicationSlotConfigured(slotName)) {
      createReplicationSlot(slotName);
    }

    PGConnection pgConnection = null;
    connection = getConnection(this.uri, this.properties);
    pgConnection = connection.unwrap(PGConnection.class);

    LogSequenceNumber lsn = null;
    if (startOffset == null || startOffset.isEmpty()) {
      if (this.offset == null) {
        if (restartLsn != null) {
          this.offset = restartLsn;
        } else {
          this.offset = "0/0";
        }
      }
      lsn = LogSequenceNumber.valueOf(this.offset);
    } else {
      lsn = LogSequenceNumber.valueOf(startOffset);
    }

    /* TODO: These are wal2json options only. Want to implement a factory based on config.
     */
    stream = pgConnection
        .getReplicationAPI()
        .replicationStream()
        .logical()
        .withSlotName(slotName)
        .withStartPosition(lsn)
        .withSlotOption("include-xids", true)
        .withSlotOption("include-timestamp", true)
        .withSlotOption("include-lsn", true)
        .withStatusInterval(100, TimeUnit.MILLISECONDS)
        .start();

    /* TODO - known issue with creation of replication API and potential NPE if
    * forceUpdateStatus() called too soon. */
    ThreadUtil.sleep(100L);
    stream.forceUpdateStatus();
    LOG.debug("Receiving changes from LSN: {}", stream.getLastReceiveLSN().asString());
    return stream.getLastReceiveLSN().asString();
  }

  public void dropReplicationSlot(String slotName)
      throws StageException
  {
    try {
      if (isReplicationSlotActive(slotName)) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(
            "select pg_terminate_backend(active_pid) from pg_replication_slots "
                + "where active = true and slot_name = ?")) {
          preparedStatement.setString(1, slotName);
          preparedStatement.execute();
        }

        waitStopReplicationSlot(slotName);
      }

      try (Connection localConnection = DriverManager.getConnection(
          hikariConfigBean.connectionString,
          hikariConfigBean.username.get(),
          hikariConfigBean.password.get()
      )) {

        try (PreparedStatement preparedStatement = localConnection
            .prepareStatement("select pg_drop_replication_slot(slot_name) "
                + "from pg_replication_slots where slot_name = ?")) {
          preparedStatement.setString(1, slotName);
          preparedStatement.execute();
        }
      }

    } catch (SQLException e) {
      throw new StageException(JDBC_407, slotName);
    }
  }

  public  void getReplicationSlot(String slotName)
      throws StageException
  {
    try {
      try (Connection localConnection = DriverManager.getConnection(
          hikariConfigBean.connectionString,
          hikariConfigBean.username.get(),
          hikariConfigBean.password.get()
      )) {
        try (PreparedStatement preparedStatement = localConnection
            .prepareStatement("select plugin, slot_type, active, "
                + "restart_lsn from "
                + "      pg_replication_slots where slot_name = ?")) {
          preparedStatement.setString(1, slotName);
          try (ResultSet rs = preparedStatement.executeQuery()) {

            while (rs.next()) {
              this.configuredPlugin = rs.getString("plugin");
              this.configuredSlotType = rs.getString("slot_type");
              this.slotActive = rs.getBoolean("active");
              this.restartLsn = rs.getString("restart_lsn");
            }
          }
        }
      }

    } catch (SQLException e) {
      throw new StageException(JDBC_407, slotName);
    }
  }

  public  boolean isReplicationSlotActive(String slotName)
      throws StageException
  {
    getReplicationSlot(slotName);
    return slotActive;
  }

  public boolean isReplicationSlotConfigured(String slotName) throws
      StageException {
    if (configuredPlugin == null) {
      getReplicationSlot(slotName);
    }
    return configuredPlugin != null;
  }

  private  void waitStopReplicationSlot(String slotName)
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

  public void setLsnFlushed(LogSequenceNumber lsn) throws StageException {

    if (lsn == null) {
      return;
    }

    stream.setAppliedLSN(lsn);
    stream.setFlushedLSN(lsn);
    try {
      stream.forceUpdateStatus();
    } catch (SQLException e) {
      LOG.error("Error forcing update status: {}", e.getMessage());
      throw new StageException(JDBC_00, " forceUpdateStatus failed :"+e.getMessage());
    }
  }

  public void openReplicationConnection() throws Exception {
    connection = getConnection(this.uri, this.properties);
  }

  public PostgresCDCWalReceiver(
      PostgresCDCConfigBean configBean,
      HikariPoolConfigBean hikariConfigBean,
      Stage.Context context
  ) throws StageException {
    this.configBean = configBean;
    this.hikariConfigBean = hikariConfigBean;
    this.context = context;

    /* TODO resolve issue with using internal Jdbc Read only connection - didn't work
     with postgres replication connection - keeping HikariConfigBean for now */
    try {
      this.connection = getConnection(hikariConfigBean.connectionString,
          hikariConfigBean.username.get(),
          hikariConfigBean.password.get());
    } catch (SQLException e) {
      throw new StageException((JDBC_00));
    }

    this.slotName = configBean.slot;
    this.outputPlugin = configBean.decoderValue;
    this.uri = hikariConfigBean.connectionString;
    this.configuredPlugin = null;
    this.configuredSlotType = null;
    this.slotActive = false;
    this.restartLsn = null ;

    this.properties = new Properties();
    PGProperty.USER.set(properties, hikariConfigBean.username.get());
    PGProperty.PASSWORD.set(properties, hikariConfigBean.password.get());
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, configBean.minVersion.getLabel());
    PGProperty.REPLICATION.set(properties, configBean.replicationType);
    PGProperty.PREFER_QUERY_MODE.set(properties, "simple");

  }

}
