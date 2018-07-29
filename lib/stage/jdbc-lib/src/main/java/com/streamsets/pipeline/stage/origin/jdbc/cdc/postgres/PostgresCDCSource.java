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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.parser.sql.DateTimeColumnHandler;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaAndTable;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresCDCSource extends BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(
      PostgresCDCSource.class);
  private static final String PREFER_QUERY_MODE = "simple";
  private static final String HIKARI_CONFIG_PREFIX = "hikariConf.";
  private static final String DRIVER_CLASSNAME = HIKARI_CONFIG_PREFIX + "driverClassName";
  private static final String USERNAME = HIKARI_CONFIG_PREFIX + "username";
  private static final String CONNECTION_STR = HIKARI_CONFIG_PREFIX + "connectionString";
  private static final int MAX_RECORD_GENERATION_ATTEMPTS = 100;
  private static final String PREFIX = "postgres.cdc.";
  private static final String LSN = PREFIX + "lsn";
  private static final String XID = PREFIX + "xid";
  private static final String TIMESTAMP_HEADER = PREFIX + "timestamp";
  private static final String TABLE = "table";
  private static final String SCHEMA = "schema";
  private static PostgresChangeTypeValues changeType;
  private Record dummyRecord;
  private final PostgresCDCConfigBean configBean;
  private final HikariPoolConfigBean hikariConfigBean;
  private volatile boolean generationStarted = false;
  private volatile boolean runnerCreated = false;
  private ErrorRecordHandler errorRecordHandler;
  private boolean containerized = false;
  private HikariDataSource dataSource = null;
  private Connection connection = null;
  private PostgresCDCWalReceiver walReceiver = null;
  private String offset = null;
  private Queue<PostgresWalRecord> cdcQueue;
  private SafeScheduledExecutorService scheduledExecutor;
  private PostgresWalRunner postgresWalRunner;
  private DateTimeColumnHandler dateTimeColumnHandler;
  private LocalDateTime startDate;
  private ZoneId zoneId;


  public PostgresCDCSource(HikariPoolConfigBean hikariConf, PostgresCDCConfigBean postgresCDCConfigBean) {
    this.configBean = postgresCDCConfigBean;
    this.hikariConfigBean = hikariConf;
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public DateTimeColumnHandler getDateTimeColumnHandler() {
    return dateTimeColumnHandler;
  }

  public LocalDateTime getStartDate() {
    return startDate;
  }

  public PostgresCDCConfigBean getConfigBean() {
    return configBean;
  }

  public HikariPoolConfigBean getHikariConfigBean() {
    return hikariConfigBean;
  }

  public String getOffset() {
    return offset;
  }

  public void setOffset(String offset) {
    this.offset = offset;
  }

  @Override
  public List<ConfigIssue> init() {
    // Call super init
    List<ConfigIssue> issues = super.init();

    // default record handler
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    // Validate the HikarConfig driverClassName
    if (!hikariConfigBean.driverClassName.isEmpty()) {
      try {
        Class.forName(hikariConfigBean.driverClassName);
      } catch (ClassNotFoundException e) {
        LOG.error("Hikari Driver class not found.", e);
        issues.add(getContext().createConfigIssue(
            "PostgreSQL CDC", DRIVER_CLASSNAME, JdbcErrors.JDBC_28, e.toString()));
      }
    }

    // Validate the HikariConfigBean
    issues = hikariConfigBean.validateConfigs(getContext(), issues);

    // Validate the PostgresCDC ConfigBean
    validatePostgresCDCConfigBean(configBean).ifPresent(issues::addAll);

    try {
      walReceiver = new PostgresCDCWalReceiver(configBean, hikariConfigBean, getContext());
      // Schemas and Tables
      if ( ! configBean.baseConfigBean.schemaTableConfigs.isEmpty()) {
        walReceiver.validateSchemaAndTables(configBean.baseConfigBean.schemaTableConfigs)
            .ifPresent(issues::addAll);
      }
      offset = walReceiver.createReplicationStream(offset);
    } catch (StageException | InterruptedException | SQLException  | TimeoutException e) {
      LOG.error("Error while connecting to DB", e);
      issues.add(getContext()
          .createConfigIssue(Groups.JDBC.name(), CONNECTION_STR, JdbcErrors.JDBC_00, e.toString()));
      return issues;
    }

    // Setup the SDC record queue as LinkedBlockingQueue of 2x MaxBatchSize
    cdcQueue = new LinkedList<>();

    return issues;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, final BatchMaker batchMaker) throws StageException {

    Long offsetAsLong = Long.valueOf(0);

    if (dummyRecord == null) {
      dummyRecord = getContext().createRecord("DUMMY");
    }

    final int batchSize = Math.min(configBean.baseConfigBean.maxBatchSize, maxBatchSize);
    int recordGenerationAttempts = 0;
    boolean recordsProduced = false;
    if (lastSourceOffset != null) {
      setOffset(StringUtils.trimToEmpty(lastSourceOffset));
    }

    if (getOffset() != null) {
      offsetAsLong = LogSequenceNumber.valueOf(getOffset()).asLong();
    }

    PostgresWalRecord postgresWalRecord = null;

    if ( ! runnerCreated) {
      createRunner();
      runnerCreated = true;
    }

    if (( ! generationStarted ) && runnerCreated) {
      startGeneration();
      generationStarted = true;
    }

    while (generationStarted &&
          !getContext().isStopped() &&
          !recordsProduced &&
          recordGenerationAttempts++ < MAX_RECORD_GENERATION_ATTEMPTS) {

      postgresWalRecord = cdcQueue.poll();

      if ((postgresWalRecord != null) &&
          (postgresWalRecord.getLsn().asLong() > offsetAsLong)) {

        final Record record = processWalRecord(postgresWalRecord);
        if (record != null) {
          Map<String, String> attributes = new HashMap<>();
          attributes.put(LSN, postgresWalRecord.getLsn().asString());
          attributes.put(XID, postgresWalRecord.getXid());
          attributes.put(TIMESTAMP_HEADER, postgresWalRecord.getTimestamp());
          attributes.forEach((k, v) -> record.getHeader().setAttribute(k, v));

          batchMaker.addRecord(record);
          walReceiver.setLsnFlushed(postgresWalRecord.getLsn());
          this.setOffset(postgresWalRecord.getLsn().asString());
        }
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted wait");
      }
    }
    return getOffset();
  }

  private Record processWalRecord(PostgresWalRecord postgresWalRecord) {
    Source.Context context = getContext();
    Field field = postgresWalRecord.getField();
    field.getValueAsMap();
    Record record = context.createRecord(field.getValueAsMap().get("xid").getValueAsString());
    record.set(postgresWalRecord.getField());
    return record;
  }


  private Optional<List<ConfigIssue>> validatePostgresCDCConfigBean(PostgresCDCConfigBean configBean) {
    List<ConfigIssue> issues = new ArrayList<>();

    if (configBean.minVersion == null) {
      this.getConfigBean().minVersion = PgVersionValues.NINEFOUR;
    }

    if (configBean.decoderValue == null) {
      this.getConfigBean().decoderValue = DecoderValues.WAL2JSON;
    }

    if (configBean.replicationType == null ) {
      this.getConfigBean().replicationType = "database";
    }

    switch(configBean.startValue) {

      case LSN:
        //Validate startLSN
        if (configBean.startLSN == null ||
            configBean.startLSN.isEmpty() ||
            (LogSequenceNumber.valueOf(configBean.startLSN).equals(LogSequenceNumber.INVALID_LSN))
            ) {
          issues.add(
              getContext().createConfigIssue(
                  Groups.CDC.name(),
                  configBean.startLSN+" is invalid LSN.",
                  JdbcErrors.JDBC_408
              )
          );
          this.setOffset("0/0"); //Valid "non-LSN" LSN or set to latest.
        } else {
          this.setOffset(configBean.startLSN);
        }
        break;

      case DATE:
        //Validate startDate
        zoneId = ZoneId.of(configBean.dbTimeZone);
        dateTimeColumnHandler = new DateTimeColumnHandler(zoneId);
        try {
          startDate = LocalDateTime.parse(
              configBean.startDate, DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss")
          );
          /* Valid offset that should be as early as possible to get the most number of WAL
          records available for the date filter to process. */
          this.setOffset(LogSequenceNumber.valueOf(1L).asString());
        } catch (DateTimeParseException e) {
          issues.add(
              getContext().createConfigIssue(
                  Groups.CDC.name(),
                  configBean.startDate+" doesn't parse as DateTime.",
                  JdbcErrors.JDBC_408
              )
          );
        }
        break;

      case LATEST:
        this.setOffset("0/0"); //Valid "non-LSN" LSN or set to latest.
        break;

      default:
        //Should never happen
        issues.add(
            getContext().createConfigIssue(
                Groups.CDC.name(),
                configBean.startValue.getLabel(),
                JdbcErrors.JDBC_408
            )
        );
    }

    return Optional.ofNullable(issues);
  }

  public void createRunner() {
    postgresWalRunner = new PostgresWalRunner(this);
  }

  public PostgresWalRunner getRunner() {
    return postgresWalRunner;
  }

  public PostgresCDCWalReceiver getWalReceiver() {
    return walReceiver;
  }

  public void startGeneration() {
    scheduledExecutor = new SafeScheduledExecutorService(1, "postgresCDC");
    scheduledExecutor.scheduleAtFixedRate(getRunner(), configBean.pollInterval, configBean.pollInterval,
        TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  public Queue<PostgresWalRecord> getQueue() {
    return cdcQueue;
  }

  @Override
  public void destroy() {
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
      try {
        scheduledExecutor.awaitTermination(5, TimeUnit.MINUTES);
      } catch (InterruptedException ex) {
        LOG.error("Interrupted while attempting to shutdown runner thread", ex);
        Thread.currentThread().interrupt();
      }
    }
    if (configBean.removeSlotOnClose) {
      try {
        getWalReceiver().dropReplicationSlot(configBean.slot);
      } catch (StageException e) {
        LOG.error(JdbcErrors.JDBC_406.getMessage(), configBean.slot);
      }
    }

  }
}
