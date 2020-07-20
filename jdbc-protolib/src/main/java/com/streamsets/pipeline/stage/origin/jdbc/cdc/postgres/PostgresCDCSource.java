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

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.parser.sql.DateTimeColumnHandler;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PostgresCDCSource extends BaseSource implements OffsetCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresCDCSource.class);
  private static final String HIKARI_CONFIG_PREFIX = "hikariConf.";
  private static final String DRIVER_CLASSNAME = HIKARI_CONFIG_PREFIX + "driverClassName";
  private static final String CONNECTION_STR = HIKARI_CONFIG_PREFIX + "connectionString";
  private static final String PREFIX = "postgres.cdc.";
  private static final String LSN = PREFIX + "lsn";
  private static final String XID = PREFIX + "xid";
  private static final String TIMESTAMP_HEADER = PREFIX + "timestamp";
  private final PostgresCDCConfigBean configBean;
  private final HikariPoolConfigBean hikariConfigBean;
  private PostgresCDCWalReceiver walReceiver = null;
  private BlockingQueue<PostgresWalRecord> cdcQueue;
  private Map<String, Object> cdcMetrics;
  private SafeScheduledExecutorService cdcExecutor;
  private Future<?> cdcGeneratorFuture;

  /**
   * The initial offset according the the configuration. Used only for the first run of the pipeline (aka when
   * creating the replication slot).
   */
  private String configInitialOffset = null;

  private DateTimeColumnHandler dateTimeColumnHandler;
  private LocalDateTime startDate;
  private ZoneId zoneId;

  /*
      The Postgres WAL (Write Ahead Log) uses a XLOG sequence number to
      track what has been committed etc and this number is used for CDC.

      This is presented via a helper class LogSequenceNumber (LSN) which
      internally is a Long representing the WAL segment and the offset into
      that segment, portrayed in String as "X/yyyyyy". "0/0" is invalid, hence
      if selecting "Initial change: fromLSN" (vs fromDate or latestChange) then
      a valid LSN must be used. This value is our default in this case.
   */
  public static final String SEED_LSN = "0/1";


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

  public String getConfigInitialOffset() {
    return configInitialOffset;
  }

  private void setConfigInitialOffset(String configInitialOffset) {
    this.configInitialOffset = configInitialOffset;
  }

  @Override
  public List<ConfigIssue> init() {
    // Call super init
    List<ConfigIssue> issues = super.init();

    // default record handler
    ErrorRecordHandler errorRecordHandler = new DefaultErrorRecordHandler(getContext());

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

    // Validate the PostgresCDC ConfigBean and sets offset to what
    // it would be its value on the first run if the replication slot does not exists
    validatePostgresCDCConfigBean(configBean).ifPresent(issues::addAll);

    try {
      walReceiver = new PostgresCDCWalReceiver(configBean, hikariConfigBean, getContext());
      // Schemas and Tables
      if ( ! configBean.baseConfigBean.schemaTableConfigs.isEmpty()) {
        walReceiver.validateSchemaAndTables().ifPresent(issues::addAll);
      }

      // giving the WAL receiver the initial offset to set it postgres in case it is creating the replication slot,
      // if the replication slot exists already, the initial offset from the configuration is ignored. The WAL
      // receiver gives us back the current offset (either the initial one or the offset that was stored in postgres).
      LogSequenceNumber lsn = walReceiver.createReplicationStream(getConfigInitialOffset());

      LOG.debug("WAL Receiver will start reading from '{}'", lsn.asString());

    } catch (StageException | InterruptedException | SQLException  | TimeoutException e) {
      LOG.error("Error while connecting to DB", e);
      issues.add(getContext()
          .createConfigIssue(
              Groups.JDBC.name(),
              CONNECTION_STR,
              JdbcErrors.JDBC_00,
              e.toString()
          )
      );

      return issues;
    } catch (Exception ex) {
      LOG.error("Error while trying to create the WAL receiver: {}", ex, ex);
      issues.add(getContext()
          .createConfigIssue(
              Groups.CDC.name(),
              "",
              JdbcErrors.JDBC_413,
              ex.toString()
          )
      );
    }

    if (issues.isEmpty()) {
      this.cdcQueue = new LinkedBlockingQueue<>(configBean.generatorQueueMaxSize);
      this.cdcExecutor = new SafeScheduledExecutorService(1, "Postgres CDC Generator");
      this.cdcGeneratorFuture = cdcExecutor.schedule(getCdcReadRunnable(), 1, TimeUnit.SECONDS);
      this.cdcMetrics = getContext().createGauge("CDC Metrics").getValue();
      updateCDCMetrics();
    }
    return issues;
  }

  private boolean isBatchDone(int currentBatchSize, int maxBatchSize, long startTime) {
    return getContext().isStopped() ||
        currentBatchSize >= maxBatchSize || // batch is full
        System.currentTimeMillis() - startTime >= configBean.maxBatchWaitTime;
  }

  @Override
  public void commit(String offset) throws StageException {
    //we simply ask the wal receiver to commit its current position which matches with the offset.
    walReceiver.commitCurrentOffset();
  }

  private Runnable getCdcReadRunnable() {
    return () -> {
      try {
        while (!getContext().isStopped()) {
          PostgresWalRecord record = getWalReceiver().read();
          if (record != null) {
            cdcQueue.put(record);
            updateCDCMetrics();
          } else {
            // No data, wait for polling
            Thread.sleep(TimeUnit.SECONDS.toMillis(configBean.pollInterval));
          }
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted generator thread during polling");
      } catch (Exception e) {
        LOG.error("Error from CDC generator", e);
        throw new RuntimeException(e);
      }
    };
  }

  private void updateCDCMetrics() {
    cdcMetrics.put("Queue Size",  cdcQueue.size());
    cdcMetrics.put("Queue Capacity", cdcQueue.remainingCapacity());
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, final BatchMaker batchMaker) throws StageException {
    // the offset given by data collector is ignored as the external system (postgres) keeps track of it.
    PostgresWalRecord postgresWalRecord = null;
    maxBatchSize = Math.min(configBean.baseConfigBean.maxBatchSize, maxBatchSize);
    int currentBatchSize = 0;

    long startTime = System.currentTimeMillis();

    while (
        !isBatchDone(
            currentBatchSize,
            maxBatchSize,
            startTime
        )
    ) {
      // Check wal sender is active
      if (!walReceiver.isWalSenderActive()) {
        throw new StageException(JdbcErrors.JDBC_606);
      }

      if (cdcGeneratorFuture.isDone()) {
        try {
          cdcGeneratorFuture.get();
          return lastSourceOffset;
        } catch (Exception e) {
          LOG.error("Error during generator thread", e);
          throw new StageException(JdbcErrors.JDBC_607, e.getMessage());
        }
      }

      long timeElapsed = System.currentTimeMillis() - startTime;
      long waitTimeMillis = Math.max(1000, configBean.maxBatchWaitTime - timeElapsed);

      try {
        postgresWalRecord = cdcQueue.poll(waitTimeMillis, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted during queue cdc poll");
        Thread.currentThread().interrupt();
        return lastSourceOffset;
      }

      if (postgresWalRecord == null) {
        LOG.trace("Received null postgresWalRecord");
      } else {
        //sets next LSN - to be flushed at end of this batch
        getWalReceiver().setNextLSN(LogSequenceNumber.valueOf(postgresWalRecord.getNextLSN()));
        updateCDCMetrics();

        // filter out non data records or old data records
        PostgresWalRecord dataRecord = WalRecordFilteringUtils.filterRecord(postgresWalRecord, this);
        if (dataRecord == null) {
          LOG.debug("Received CDC with LSN {} from stream value filtered out", postgresWalRecord.getLsn().asString());
        } else {
          String recordLsn = dataRecord.getLsn().asString();
          LOG.debug("Received CDC with LSN {} from stream value", recordLsn);

          if (LOG.isTraceEnabled()) {
            LOG.trace("Valid CDC: {} ", dataRecord);
          }

          final Record record = processWalRecord(dataRecord);

          Record.Header header = record.getHeader();

          header.setAttribute(LSN, recordLsn);
          header.setAttribute(XID, dataRecord.getXid());
          header.setAttribute(TIMESTAMP_HEADER, dataRecord.getTimestamp());

          batchMaker.addRecord(record);
          currentBatchSize++;
        }
      }
    }
    // we report the current position of the WAL reader.
    return "dummy-not-used";
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

    if (TimeUnit.SECONDS.toMillis(configBean.pollInterval) > configBean.maxBatchWaitTime) {
      issues.add(
          getContext().createConfigIssue(
              Groups.CDC.name(),
              "postgresCDCConfigBean.pollInterval",
              JdbcErrors.JDBC_412, configBean.pollInterval, configBean.maxBatchWaitTime)
      );
    }

    switch(configBean.startValue) {

      case LSN:
        //Validate startLSN
        if (configBean.startLSN == null ||
            configBean.startLSN.isEmpty() ||
            (LogSequenceNumber.valueOf(configBean.startLSN).equals(LogSequenceNumber.INVALID_LSN))
        ) {
          issues.add(
              getContext().createConfigIssue(Groups.CDC.name(),
                  "postgresCDCConfigBean.startLSN",
                  JdbcErrors.JDBC_408)
          );
        } else {
          this.setConfigInitialOffset(configBean.startLSN);
        }
        break;

      case DATE:
        //Validate startDate
        zoneId = ZoneId.of(configBean.dbTimeZone);
        dateTimeColumnHandler = new DateTimeColumnHandler(zoneId, configBean.convertTimestampToString);
        try {
          startDate = LocalDateTime.parse(
              configBean.startDate, DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss")
          );
          /* Valid offset that should be as early as possible to get the most number of WAL
          records available for the date filter to process. */
          this.setConfigInitialOffset(LogSequenceNumber.valueOf(1L).asString());
        } catch (DateTimeParseException e) {
          issues.add(
              getContext().createConfigIssue(
                  Groups.CDC.name(),
                  "postgresCDCConfigBean.startDate",
                  JdbcErrors.JDBC_408
              )
          );
        }
        break;

      case LATEST:
        this.setConfigInitialOffset(null); //Null picks up the latestLSN.
        break;

      default:
        //Should never happen
        issues.add(
            getContext().createConfigIssue(
                Groups.CDC.name(),
                "postgresCDCConfigBean.startValue",
                JdbcErrors.JDBC_408
            )
        );
    }

    return Optional.of(issues);
  }


  public PostgresCDCWalReceiver getWalReceiver() {
    return walReceiver;
  }

  @Override
  public void destroy() {
    if (cdcGeneratorFuture != null) {
      cdcGeneratorFuture.cancel(true);
    }
    if (cdcExecutor != null) {
      cdcExecutor.shutdown();
      try {
        cdcExecutor.awaitTermination(5, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted when waiting for CDC Executor to finish");
        Thread.currentThread().interrupt();
      }
    }
    if (getWalReceiver() != null) {
      if (configBean.removeSlotOnClose) {
        try {
          getWalReceiver().dropReplicationSlot(configBean.slot);
        } catch (StageException e) {
          LOG.error(JdbcErrors.JDBC_406.getMessage(), configBean.slot);
        }
      }
      try {
        getWalReceiver().close();
      } catch (SQLException ex) {
        LOG.error("Error while closing connection: {}", ex.toString(), ex);
      }
    }
  }

}
