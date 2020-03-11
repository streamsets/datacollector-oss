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
package com.streamsets.pipeline.stage.origin.oracle.cdc;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import com.streamsets.pipeline.lib.jdbc.BasicConnectionString;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.PrecisionAndScale;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.jdbc.parser.sql.DateTimeColumnHandler;
import com.streamsets.pipeline.lib.jdbc.parser.sql.ParseUtil;
import com.streamsets.pipeline.lib.jdbc.parser.sql.SQLListener;
import com.streamsets.pipeline.lib.jdbc.parser.sql.SQLParser;
import com.streamsets.pipeline.lib.jdbc.parser.sql.SQLParserUtils;
import com.streamsets.pipeline.lib.jdbc.parser.sql.UnparseableSQLException;
import com.streamsets.pipeline.lib.jdbc.parser.sql.UnsupportedFieldTypeException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.ChangeTypeValues;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaAndTable;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean;
import com.zaxxer.hikari.HikariDataSource;
import net.jcip.annotations.GuardedBy;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.parboiled.Parboiled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_00;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_16;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_40;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_404;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_405;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_41;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_42;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_43;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_44;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_47;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_48;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_49;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_50;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_502;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_52;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_54;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_81;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_82;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_83;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_84;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_85;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_86;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_87;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.COMMIT_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.DDL_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.DELETE_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.INSERT_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.ROLLBACK_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.SELECT_FOR_UPDATE_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.UPDATE_CODE;
import static com.streamsets.pipeline.lib.jdbc.parser.sql.ParseUtil.JDBCTypeNames;
import static com.streamsets.pipeline.stage.origin.oracle.cdc.Groups.CDC;
import static com.streamsets.pipeline.stage.origin.oracle.cdc.Groups.CREDENTIALS;

public class OracleCDCSource extends BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(OracleCDCSource.class);
  private static final String CDB_ROOT = "CDB$ROOT";
  private static final String HIKARI_CONFIG_PREFIX = "hikariConf.";
  private static final String DRIVER_CLASSNAME = HIKARI_CONFIG_PREFIX + "driverClassName";
  private static final String USERNAME = HIKARI_CONFIG_PREFIX + "username";
  private static final String CONNECTION_STR = HIKARI_CONFIG_PREFIX + "connectionString";
  private static final String CURRENT_SCN = "SELECT CURRENT_SCN FROM GV$DATABASE";
  // At the time of executing this statement, either the cachedSCN is 0
  // (which means we are executing for the first time), or it is no longer valid, so select
  // only the ones that are > than the cachedSCN.
  private static final String
      GET_OLDEST_SCN
      = "SELECT FIRST_CHANGE#, STATUS from GV$ARCHIVED_LOG WHERE STATUS = 'A' AND FIRST_CHANGE# > ? ORDER BY " +
      "FIRST_CHANGE#";
  private static final String SWITCH_TO_CDB_ROOT = "ALTER SESSION SET CONTAINER = CDB$ROOT";
  private static final String ROWID = "ROWID";
  private static final String PREFIX = "oracle.cdc.";
  public static final String SSN = PREFIX + "SSN";
  public static final String RS_ID = PREFIX + "RS_ID";
  private static final String SCN = PREFIX + "scn";
  private static final String USER = PREFIX + "user";
  private static final String DDL_TEXT = PREFIX + "ddl";
  private static final String TIMESTAMP_HEADER = PREFIX + "timestamp";
  private static final String TABLE = PREFIX + "table";
  private static final String ROWID_KEY = PREFIX + "rowId";
  private static final String QUERY_KEY = PREFIX + "query";
  private static final String NULL = "NULL";
  private static final String VERSION_STR = "v2";
  private static final String VERSION_UNCOMMITTED = "v3";
  private static final String ZERO = "0";
  private static final String SCHEMA = "schema";
  private static final int MAX_RECORD_GENERATION_ATTEMPTS = 100;

  // What are all these constants?
  // String templates used in debug logging statements. To avoid unnecessarily creating new strings,
  // we just reuse these constants to avoid adding an if(LOG.isDebugEnabled()) call.
  private static final String START_DATE_REFRESHED_TO = "Start date refreshed to: '{}'";
  private static final String
      TRYING_TO_START_LOG_MINER_WITH_START_DATE_AND_END_DATE
      = "Trying to start LogMiner with start date: {} and end date: {}";
  private static final String
      TRYING_TO_START_LOG_MINER_WITH_START_SCN_AND_END_SCN
      = "Trying to start LogMiner with start SCN: {} and end SCN: {}";
  private static final String START_TIME_END_TIME = "Start time = {}, End time = {}";
  private static final String XID = PREFIX + "xid";
  private static final String SEQ = "SEQ";
  private static final HashQueue<RecordSequence> EMPTY_LINKED_HASHSET = new InMemoryHashQueue<>(0);

  private static final String SENDING_TO_ERROR_AS_CONFIGURED = ". Sending to error as configured";
  private static final String UNSUPPORTED_TO_ERR = JDBC_85.getMessage() + SENDING_TO_ERROR_AS_CONFIGURED;
  private static final String DISCARDING_RECORD_AS_CONFIGURED = ". Discarding record as configured";
  private static final String UNSUPPORTED_DISCARD = JDBC_85.getMessage() + DISCARDING_RECORD_AS_CONFIGURED;
  private static final String UNSUPPORTED_SEND_TO_PIPELINE = JDBC_85.getMessage() +
      ". Sending to pipeline as configured";
  private static final String GENERATED_RECORD = "Generated Record: '{}' in transaction Id {}";
  public static final String FOUND_RECORDS_IN_TRANSACTION = "Found {} records in transaction ID {}";
  public static final String
      STARTED_LOG_MINER_WITH_START_SCN_AND_END_SCN
      = "Started LogMiner with start SCN: {} and end SCN: {}";
  public static final String REDO_SELECT_QUERY = "Redo select query for selectFromLogMnrContents = {}";
  public static final String CURRENT_LATEST_SCN_IS = "Current latest SCN is: {}";
  private static final String
      SESSION_WINDOW_CURRENT_MSG
      = "Session window is in current time. Fetch size now set to {}";
  private static final String ROLLBACK_MESSAGE = "got rollback for {} - transaction discarded.";
  public static final int LOGMINER_START_MUST_BE_CALLED = 1306;
  public static final String ROLLBACK = "rollback";
  public static final String SKIP = "skip";
  public static final String ONE = "1";
  public static final String READ_NULL_QUERY_FROM_ORACLE = "Read (null) query from Oracle with SCN: {}, Txn Id: {}";
  private DateTimeColumnHandler dateTimeColumnHandler;


  private boolean sentInitialSchemaEvent = false;
  private File txnBufferLocation;

  private PreparedStatement selectFromLogMnrContents;
  private static final int MISSING_LOG_FILE = 1291;
  private static final int QUERY_TIMEOUT = 1013;

  private final Lock bufferedRecordsLock = new ReentrantLock();
  private final BlockingQueue<StageException> stageExceptions = new LinkedBlockingQueue<>(1);

  @GuardedBy(value = "bufferedRecordsLock")
  private final Map<TransactionIdKey, HashQueue<RecordSequence>> bufferedRecords = new HashMap<>();
  private final Map<TransactionIdKey, List<String>> rollbacks = new HashMap<>();

  private final AtomicReference<BigDecimal> cachedSCNForRedoLogs = new AtomicReference<>(BigDecimal.ZERO);

  private BlockingQueue<RecordOffset> recordQueue;
  private final BlockingQueue<RecordErrorString> errorRecords = new LinkedBlockingQueue<>();
  private final BlockingQueue<String> unparseable = new LinkedBlockingQueue<>();
  private final BlockingQueue<RecordTxnInfo> expiredRecords = new LinkedBlockingQueue<>();
  private final BlockingQueue<ErrorAndCause> otherErrors = new LinkedBlockingQueue<>();


  private String selectString;
  private String version;
  private ZoneId zoneId;
  private Record dummyRecord;
  private boolean useLocalBuffering;

  private Gauge<Map<String, Object>> delay;
  private CallableStatement startLogMnrSCNToDate;
  private SshTunnelService sshTunnelService;

  private static final String
      CONFIG_PROPERTY
      = "com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle.addrecordstoqueue";
  private static final boolean CONFIG_PROPERTY_DEFAULT_VALUE = false;
  private boolean useNewAddRecordsToQueue;

  private enum DDL_EVENT {
    CREATE,
    ALTER,
    DROP,
    TRUNCATE,
    STARTUP, // Represents event sent at startup.
    UNKNOWN
  }

  private static final String
      GET_TIMESTAMPS_FROM_LOGMNR_CONTENTS
      = "SELECT TIMESTAMP FROM V$LOGMNR_CONTENTS ORDER BY TIMESTAMP";
  private static final String OFFSET_DELIM = "::";
  private static final int RESULTSET_CLOSED_AS_LOGMINER_SESSION_CLOSED = 1306;
  private static final String NLS_DATE_FORMAT = "ALTER SESSION SET NLS_DATE_FORMAT = " +
      DateTimeColumnHandler.DT_SESSION_FORMAT;
  private static final String NLS_NUMERIC_FORMAT = "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = \'.,\'";
  private static final String NLS_TIMESTAMP_FORMAT = "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = " +
      DateTimeColumnHandler.TIMESTAMP_SESSION_FORMAT;
  private static final String NLS_TIMESTAMP_TZ_FORMAT = "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = " +
      DateTimeColumnHandler.ZONED_DATETIME_SESSION_FORMAT;
  private final Pattern ddlPattern = Pattern.compile("(CREATE|ALTER|DROP|TRUNCATE).*", Pattern.CASE_INSENSITIVE);

  private static final String TABLE_METADATA_TABLE_SCHEMA_CONSTANT = "TABLE_SCHEM";
  private static final String TABLE_METADATA_TABLE_NAME_CONSTANT = "TABLE_NAME";

  private final OracleCDCConfigBean configBean;
  private final HikariPoolConfigBean hikariConfigBean;
  private final Map<SchemaAndTable, Map<String, Integer>> tableSchemas = new HashMap<>();
  private final Map<SchemaAndTable, Map<String, String>> dateTimeColumns = new HashMap<>();
  private final Map<SchemaAndTable, Map<String, PrecisionAndScale>> decimalColumns = new HashMap<>();
  private final Map<SchemaAndTable, BigDecimal> tableSchemaLastUpdate = new HashMap<>();

  private final ExecutorService
      generationExecutor
      = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Oracle CDC Data Generator")
                                                                    .build());

  private volatile boolean generationStarted = false;
  private final boolean shouldTrackDDL;

  private String logMinerProcedure;
  private ErrorRecordHandler errorRecordHandler;
  private boolean containerized = false;

  private HikariDataSource dataSource = null;
  private Connection connection = null;
  private PreparedStatement getOldestSCN;
  private PreparedStatement getLatestSCN;
  private CallableStatement startLogMnrForCommitSCN;
  private CallableStatement startLogMnrForData;
  private CallableStatement endLogMnr;
  private PreparedStatement dateStatement;
  private PreparedStatement tsStatement;
  private PreparedStatement numericFormat;
  private PreparedStatement switchContainer;
  private PreparedStatement getTimestampsFromLogMnrContents;
  private PreparedStatement tsTzStatement;

  private final ThreadLocal<ParseTreeWalker> parseTreeWalker = ThreadLocal.withInitial(ParseTreeWalker::new);
  private final ThreadLocal<SQLListener> sqlListener = ThreadLocal.withInitial(SQLListener::new);
  private final ThreadLocal<SQLParser>
      sqlParser
      = ThreadLocal.withInitial(() -> Parboiled.createParser(SQLParser.class));

  private ExecutorService parsingExecutor;

  private final JdbcUtil jdbcUtil;

  private boolean checkBatchSize = true;

  public OracleCDCSource(HikariPoolConfigBean hikariConf, OracleCDCConfigBean oracleCDCConfigBean) {
    this.jdbcUtil = UtilsProvider.getJdbcUtil();
    this.configBean = oracleCDCConfigBean;
    this.hikariConfigBean = hikariConf;
    this.shouldTrackDDL = configBean.dictionary == DictionaryValues.DICT_FROM_REDO_LOGS;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, final BatchMaker batchMaker) throws StageException {
    if (dummyRecord == null) {
      dummyRecord = getContext().createRecord("DUMMY");
    }
    final int batchSize = Math.min(configBean.baseConfigBean.maxBatchSize, maxBatchSize);
    if (checkBatchSize && configBean.baseConfigBean.maxBatchSize > maxBatchSize) {
      getContext().reportError(JDBC_502, maxBatchSize);
      checkBatchSize = false;
    }

    int recordGenerationAttempts = 0;
    boolean recordsProduced = false;
    String nextOffset = StringUtils.trimToEmpty(lastSourceOffset);
    pollForStageExceptions();
    while (!getContext().isStopped() &&
        !recordsProduced &&
        recordGenerationAttempts++ < MAX_RECORD_GENERATION_ATTEMPTS) {
      sshTunnelService.healthCheck();
      if (!sentInitialSchemaEvent) {
        for (SchemaAndTable schemaAndTable : tableSchemas.keySet()) {
          getContext().toEvent(createEventRecord(DDL_EVENT.STARTUP, null, schemaAndTable, ZERO, true, null));
        }
        sentInitialSchemaEvent = true;
      }

      try {
        if (!generationStarted) {
          startGeneratorThread(lastSourceOffset);
        }
      } catch (StageException ex) {
        LOG.error("Error while attempting to produce records", ex);
        throw ex;
      } catch (Exception ex) {
        // In preview, destroy gets called after timeout which can cause a SQLException
        if (getContext().isPreview() && ex instanceof SQLException) {
          LOG.warn("Exception while previewing", ex);
          return NULL;
        }
        LOG.error("Error while attempting to produce records", ex);
        errorRecordHandler.onError(JDBC_44, Throwables.getStackTraceAsString(ex));
      }

      int batchCount = 0;

      while (batchCount < batchSize) {
        try {
          RecordOffset recordOffset = recordQueue.poll(1, TimeUnit.SECONDS);
          if (recordOffset != null) {
            if (recordOffset.record instanceof EventRecord) {
              getContext().toEvent((EventRecord) recordOffset.record);
            } else {
              // Make sure we move the offset forward even if no real data was produced, so that we don't end up
              // pointing to a start time which is very old.
              if (recordOffset.record != dummyRecord) {
                batchMaker.addRecord(recordOffset.record);
                recordsProduced = true;
                batchCount++;
              }
            }
            nextOffset = recordOffset.offset.toString();
          } else {
            break;
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          throw new StageException(JDBC_87, ex);
        }
      }
    }
    sendErrors();
    pollForStageExceptions();
    return nextOffset;
  }

  private void sendErrors() throws StageException {
    handleErrors(errorRecords.iterator(),
        r -> getContext().toError(r.record, JDBC_85, r.errorString, r.record.getHeader().getAttribute(TABLE))
    );

    handleErrors(unparseable.iterator(), r -> {
      try {
        errorRecordHandler.onError(JDBC_43, r);
      } catch (StageException e) {
        addToStageExceptionsQueue(e);
      }
    });

    handleErrors(expiredRecords.iterator(), r -> getContext().toError(r.record, JDBC_84, r.txnId, r.txnStartTime));

    handleErrors(otherErrors.iterator(), r -> {
      try {
        errorRecordHandler.onError(r.error, r.ex);
      } catch (StageException e) {
        addToStageExceptionsQueue(e);
      }
    });
  }

  private <E> void handleErrors(Iterator<E> iterator, Consumer<E> consumerMethod) {
    while (iterator.hasNext()) {
      E e = iterator.next();
      iterator.remove();
      consumerMethod.accept(e);
    }
  }

  private void pollForStageExceptions() throws StageException {
    StageException ex = stageExceptions.poll();
    if (ex != null) {
      throw ex;
    }
  }

  private void startGeneratorThread(String lastSourceOffset) throws StageException, SQLException {
    Offset offset = null;
    LocalDateTime startTimestamp;
    try {
      startLogMnrForRedoDict();
      if (!StringUtils.isEmpty(lastSourceOffset)) {
        offset = new Offset(lastSourceOffset);
        if (lastSourceOffset.startsWith("v3")) {
          if (!useLocalBuffering) {
            throw new StageException(JDBC_82);
          }
          startTimestamp = offset.timestamp.minusSeconds(configBean.txnWindow);
        } else {
          if (useLocalBuffering) {
            throw new StageException(JDBC_83);
          }
          startTimestamp = getDateForSCN(new BigDecimal(offset.scn));
        }
        offset.timestamp = startTimestamp;
        adjustStartTimeAndStartLogMnr(startTimestamp);
      } else { // reset the start date only if it not set.
        if (configBean.startValue != StartValues.SCN) {
          LocalDateTime startDate;
          if (configBean.startValue == StartValues.DATE) {
            startDate = LocalDateTime.parse(configBean.startDate, dateTimeColumnHandler.dateFormatter);
          } else {
            startDate = nowAtDBTz();
          }
          startDate = adjustStartTimeAndStartLogMnr(startDate);
          offset = new Offset(version, startDate, ZERO, 0, "");
        } else {
          BigDecimal startCommitSCN = new BigDecimal(configBean.startSCN);
          startLogMnrSCNToDate.setBigDecimal(1, startCommitSCN);
          final LocalDateTime start = getDateForSCN(startCommitSCN);
          LocalDateTime endTime = getEndTimeForStartTime(start);
          startLogMnrSCNToDate.setString(2, endTime.format(dateTimeColumnHandler.dateFormatter));
          startLogMnrSCNToDate.execute();
          offset = new Offset(version, start, startCommitSCN.toPlainString(), 0, "");
        }
      }
    } catch (SQLException ex) {
      LOG.error("SQLException while trying to setup record generator thread", ex);
      generationStarted = false;
      throw new StageException(JDBC_52, ex);
    }
    final Offset os = offset;
    final PreparedStatement select = selectFromLogMnrContents;
    generationExecutor.submit(() -> {
      try {
        generateRecords(os, select);
      } catch (Throwable ex) {
        LOG.error("Error while producing records", ex);
        generationStarted = false;
      }
    });
  }

  @NotNull
  private LocalDateTime adjustStartTimeAndStartLogMnr(LocalDateTime startDate) throws SQLException, StageException {
    startDate = adjustStartTime(startDate);
    LocalDateTime endTime = getEndTimeForStartTime(startDate);
    startLogMinerUsingGivenDates(startDate.format(dateTimeColumnHandler.dateFormatter),
        endTime.format(dateTimeColumnHandler.dateFormatter)
    );
    LOG.debug(START_TIME_END_TIME, startDate, endTime);
    return startDate;
  }

  private void resetDBConnectionsIfRequired() throws StageException, SQLException {
    if (dataSource == null || dataSource.isClosed()) {
      connection = null; // Force re-init without checking validity which takes time.
      dataSource = jdbcUtil.createDataSourceForRead(hikariConfigBean);
    }
    if (connection == null || !connection.isValid(30)) {
      if (connection != null) {
        dataSource.evictConnection(connection);
      }
      connection = dataSource.getConnection();
      connection.setAutoCommit(false);
      initializeStatements();
      initializeLogMnrStatements();
      alterSession();
    }
  }

  private long getDelay(LocalDateTime lastEndTime) {
    return localDateTimeToEpoch(nowAtDBTz()) - localDateTimeToEpoch(lastEndTime);
  }

  private void generateRecords(
      Offset startingOffset, PreparedStatement selectChanges
  ) {
    // When this is called the first time, Logminer was started either from SCN or from a start date, so we just keep
    // track of the start date etc.
    LOG.info("Attempting to generate records");
    boolean error;
    StringBuilder query = new StringBuilder();
    BigDecimal lastCommitSCN = new BigDecimal(startingOffset.scn);
    int sequenceNumber = startingOffset.sequence;
    LocalDateTime startTime = adjustStartTime(startingOffset.timestamp);
    String lastTxnId = startingOffset.txnId;
    LocalDateTime endTime = getEndTimeForStartTime(startTime);
    boolean sessionWindowInCurrent = inSessionWindowCurrent(startTime, endTime);
    ResultSet resultSet = null;
    while (!getContext().isStopped()) {
      error = false;
      generationStarted = true;
      try {
        recordQueue.put(new RecordOffset(dummyRecord,
            new Offset(version, startTime, lastCommitSCN.toPlainString(), sequenceNumber, lastTxnId)
        ));
        selectChanges = getSelectChangesStatement();
        if (!useLocalBuffering) {
          selectChanges.setBigDecimal(1, lastCommitSCN);
          selectChanges.setInt(2, sequenceNumber);
          selectChanges.setBigDecimal(3, lastCommitSCN);
          if (shouldTrackDDL) {
            selectChanges.setBigDecimal(4, lastCommitSCN);
          }
        }
        selectChanges.setFetchSize(1);
        resultSet = selectChanges.executeQuery();
        if (!sessionWindowInCurrent) {
          // Overwrite fetch size when session window is past
          resultSet.setFetchSize(configBean.jdbcFetchSize);
        } else {
          if (LOG.isTraceEnabled()) {
            LOG.trace(SESSION_WINDOW_CURRENT_MSG, configBean.fetchSizeLatest);
          }
          resultSet.setFetchSize(configBean.fetchSizeLatest);
        }
        while (!getContext().isStopped() && resultSet.next()) {
          String queryFragment = resultSet.getString(5);
          BigDecimal scnDecimal = resultSet.getBigDecimal(1);
          String scn = scnDecimal.toPlainString();
          String xidUsn = String.valueOf(resultSet.getLong(10));
          String xidSlt = String.valueOf(resultSet.getString(11));
          String xidSqn = String.valueOf(resultSet.getString(12));
          String xid = xidUsn + "." + xidSlt + "." + xidSqn;
          // Query Fragment is not null -> we need to process
          // Query Fragment is null AND the query string buffered from previous rows due to CSF == 0 is null,
          // nothing to do, go to next row
          // Query Fragment is null, but there is previously buffered data in the query, go ahead and process.
          if (queryFragment != null) {
            query.append(queryFragment);
          } else if (queryFragment == null && query.length() == 0) {
            LOG.debug(READ_NULL_QUERY_FROM_ORACLE, scn, xid);
            continue;
          }

          // CSF is 1 if the query is incomplete, so read the next row before parsing
          // CSF being 0 means query is complete, generate the record
          if (resultSet.getInt(9) == 0) {
            if (query.length() == 0) {
              LOG.debug(READ_NULL_QUERY_FROM_ORACLE, scn, xid);
              continue;
            }
            String queryString = query.toString();
            query.setLength(0);
            String username = resultSet.getString(2);
            short op = resultSet.getShort(3);
            String timestamp = resultSet.getString(4);
            LocalDateTime tsDate = Timestamp.valueOf(timestamp).toLocalDateTime();
            delay.getValue().put("delay", getDelay(tsDate));
            String table = resultSet.getString(6);
            BigDecimal commitSCN = resultSet.getBigDecimal(7);
            int seq = resultSet.getInt(8);

            String rsId = resultSet.getString(13);
            Object ssn = resultSet.getObject(14);
            String schema = String.valueOf(resultSet.getString(15));
            int rollback = resultSet.getInt(16);
            String rowId = resultSet.getString(17);
            SchemaAndTable schemaAndTable = new SchemaAndTable(schema, table);
            TransactionIdKey key = new TransactionIdKey(xid);

            int bufferedRecordsSize = 0;
            int totRecs = 0;
            bufferedRecordsLock.lock();
            try {
              if (useLocalBuffering &&
                  bufferedRecords.containsKey(key) &&
                  bufferedRecords.get(key).contains(new RecordSequence(null, null, 0, 0, rsId, ssn, null))) {
                continue;
              }
              if (LOG.isDebugEnabled()) {
                bufferedRecordsSize = bufferedRecords.size();
                for (Map.Entry<OracleCDCSource.TransactionIdKey, HashQueue<RecordSequence>> r :
                    bufferedRecords.entrySet()) {
                  totRecs += r.getValue().size();
                }
              }
            } finally {
              bufferedRecordsLock.unlock();
            }
            Offset offset = null;
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Num Active Txns = {} Total Cached Records = {} Commit SCN = {}, SCN = {}, Operation = {}, Txn Id =" +
                      " {}, Timestamp = {}, Row Id = {}, Redo SQL = {}",
                  bufferedRecordsSize,
                  totRecs,
                  commitSCN,
                  scn,
                  op,
                  xid,
                  tsDate,
                  rowId,
                  queryString
              );
            }

            if (op != DDL_CODE && op != COMMIT_CODE && op != ROLLBACK_CODE) {
              if (!useLocalBuffering) {
                offset = new Offset(version, tsDate, commitSCN.toPlainString(), seq, xid);
              }
              Map<String, String> attributes = new HashMap<>();
              attributes.put(SCN, scn);
              attributes.put(USER, username);
              attributes.put(TIMESTAMP_HEADER, timestamp);
              attributes.put(TABLE, table);
              attributes.put(SEQ, String.valueOf(seq));
              attributes.put(XID, xid);
              attributes.put(RS_ID, rsId);
              attributes.put(SSN, ssn.toString());
              attributes.put(SCHEMA, schema);
              attributes.put(ROLLBACK, String.valueOf(rollback));
              attributes.put(ROWID_KEY, rowId);
              if (!useLocalBuffering || getContext().isPreview()) {
                if (commitSCN.compareTo(lastCommitSCN) < 0 ||
                    (commitSCN.compareTo(lastCommitSCN) == 0 && seq < sequenceNumber)) {
                  continue;
                }
                lastCommitSCN = commitSCN;
                sequenceNumber = seq;
                if (configBean.keepOriginalQuery) {
                  attributes.put(QUERY_KEY, queryString);
                }
                try {
                  Record record = generateRecord(queryString, attributes, op);
                  if (record != null && !record.getEscapedFieldPaths().isEmpty()) {
                    recordQueue.put(new RecordOffset(record, offset));
                  }
                } catch (UnparseableSQLException ex) {
                  LOG.error("Parsing failed", ex);
                  unparseable.offer(queryString);
                }
              } else {
                bufferedRecordsLock.lock();
                try {
                  HashQueue<RecordSequence> records = bufferedRecords.computeIfAbsent(key, x -> {
                    x.setTxnStartTime(tsDate);
                    return createTransactionBuffer(key.txnId);
                  });

                  int nextSeq = records.isEmpty() ? 1 : records.tail().seq + 1;
                  RecordSequence node = new RecordSequence(attributes, queryString, nextSeq, op, rsId, ssn, tsDate);

                  //check for SAVEPOINT/ROLLBACK here...
                  // when we get a record with the ROLLBACK indicator set,
                  // we go through all records in the transaction,
                  // find the last one with the matching rowId.
                  // save it's position if and only if, it has
                  // not previously been marked as a record to SKIP.
                  //
                  // ONLY use this code when we are using the new version of addRecordsToQueue()
                  // the old version of addRecordsToQueue() includes the rollback code
                  // and does not support "SKIP"ping Records.
                  if (node.headers.get(ROLLBACK).equals(ONE) && useNewAddRecordsToQueue) {
                    int lastOne = -1;
                    int count = 0;
                    for (RecordSequence rs : records) {
                      if (rs.headers.get(ROWID_KEY).equals(node.headers.get(ROWID_KEY)) &&
                          rs.headers.get(SKIP) == null) {
                        lastOne = count;
                      }
                      count++;
                    }
                    // go through the records again, and
                    // update the lastOne that has the specific
                    // rowId to indicate we should not process it.
                    // the original "ROLLBACK" record will *not* be
                    // added to the transaction.
                    count = 0;
                    for (RecordSequence rs : records) {
                      if (count == lastOne) {
                        rs.headers.put(SKIP, ONE);
                        break;
                      }
                      count++;
                    }
                  } else {
                    records.add(node);
                  }
                } finally {
                  bufferedRecordsLock.unlock();
                }
              }
            } else if (!getContext().isPreview() && useLocalBuffering && (op == COMMIT_CODE || op == ROLLBACK_CODE)) {
              // so this commit was previously processed or it is a rollback, so don't care.
              if (op == ROLLBACK_CODE || scnDecimal.compareTo(lastCommitSCN) < 0) {
                bufferedRecordsLock.lock();
                try {
                  bufferedRecords.remove(key);
                  LOG.info(ROLLBACK_MESSAGE, key.txnId);
                } finally {
                  bufferedRecordsLock.unlock();
                }
              } else {
                bufferedRecordsLock.lock();
                try {
                  HashQueue<RecordSequence> records = bufferedRecords.getOrDefault(key, EMPTY_LINKED_HASHSET);
                  if (lastCommitSCN.equals(scnDecimal) && xid.equals(lastTxnId)) {
                    removeProcessedRecords(records, sequenceNumber);
                  }
                  int bufferedRecordsToBeRemoved = records.size();
                  LOG.debug(FOUND_RECORDS_IN_TRANSACTION, bufferedRecordsToBeRemoved, xid);
                  lastCommitSCN = scnDecimal;
                  lastTxnId = xid;

                  if (useNewAddRecordsToQueue) {
                    sequenceNumber = addRecordsToQueue(tsDate, scn, xid);
                  } else {
                    sequenceNumber = addRecordsToQueueOLD(tsDate, scn, xid);
                  }

                } finally {
                  bufferedRecordsLock.unlock();
                }
              }
            } else {
              offset = new Offset(version, tsDate, scn, 0, xid);
              boolean sendSchema = false;
              // Commit/rollback in Preview will also end up here, so don't really do any of the following in preview
              // Don't bother with DDL events here.
              if (!getContext().isPreview()) {
                // Event is sent on every DDL, but schema is not always sent.
                // Schema sending logic:
                // CREATE/ALTER: Schema is sent if the schema after the ALTER is newer than the cached schema
                // (which we would have sent as an event earlier, at the last alter)
                // DROP/TRUNCATE: Schema is not sent, since they don't change schema.
                DDL_EVENT type = getDdlType(queryString);
                if (type == DDL_EVENT.ALTER || type == DDL_EVENT.CREATE) {
                  sendSchema = refreshSchema(scnDecimal, new SchemaAndTable(schema, table));
                }
                recordQueue.put(new RecordOffset(createEventRecord(
                    type,
                    queryString,
                    schemaAndTable,
                    offset.toString(),
                    sendSchema,
                    timestamp
                ), offset));
              }
            }
            query.setLength(0);
          }
        }
      } catch (SQLException ex) {
        error = true;
        // force a restart from the same timestamp.
        if (ex.getErrorCode() == MISSING_LOG_FILE) {
          LOG.warn("SQL Exception while retrieving records", ex);
          addToStageExceptionsQueue(new StageException(JDBC_86, ex));
        } else if (ex.getErrorCode() != RESULTSET_CLOSED_AS_LOGMINER_SESSION_CLOSED) {
          LOG.warn("SQL Exception while retrieving records", ex);
        } else if (ex.getErrorCode() == QUERY_TIMEOUT) {
          LOG.warn("LogMiner select query timed out");
        } else if (ex.getErrorCode() == LOGMINER_START_MUST_BE_CALLED) {
          LOG.warn("Last LogMiner session did not start successfully. Will retry", ex);
        } else {
          LOG.error("Error while reading data", ex);
          addToStageExceptionsQueue(new StageException(JDBC_52, ex));
        }
      } catch (StageException e) {
        LOG.error("Error while reading data", e);
        error = true;
        addToStageExceptionsQueue(e);
      } catch (InterruptedException ex) {
        LOG.error("Interrupted while waiting to add data");
        Thread.currentThread().interrupt();
      } catch (Exception ex) {
        LOG.error("Error while reading data", ex);
        error = true;
        addToStageExceptionsQueue(new StageException(JDBC_52, ex));
      } finally {
        // If an incomplete batch is seen, it means we are going to move the window forward
        // Ending this session and starting a new one helps reduce PGA memory usage.
        try {
          if (resultSet != null && !resultSet.isClosed()) {
            resultSet.close();
          }
          if (selectChanges != null && !selectChanges.isClosed()) {
            selectChanges.close();
          }
        } catch (SQLException ex) {
          LOG.warn("Error while attempting to close SQL statements", ex);
        }
        try {
          endLogMnr.execute();
        } catch (SQLException ex) {
          LOG.warn("Error while trying to close logminer session", ex);
        }
        try {
          if (error) {
            resetConnectionsQuietly();
          } else {
            discardOldUncommitted(startTime);
            startTime = adjustStartTime(endTime);
            endTime = getEndTimeForStartTime(startTime);
          }
          sessionWindowInCurrent = inSessionWindowCurrent(startTime, endTime);
          startLogMinerUsingGivenDates(startTime.format(dateTimeColumnHandler.dateFormatter),
              endTime.format(dateTimeColumnHandler.dateFormatter)
          );
        } catch (SQLException ex) {
          LOG.error("Error while attempting to start LogMiner", ex);
          addToStageExceptionsQueue(new StageException(JDBC_52, ex));
        } catch (StageException ex) {
          LOG.error("Error while attempting to start logminer for redo log dictionary", ex);
          addToStageExceptionsQueue(ex);
        }
      }
    }
  }

  private boolean inSessionWindowCurrent(LocalDateTime startTime, LocalDateTime endTime) {
    LocalDateTime currentTime = nowAtDBTz();
    return (currentTime.isAfter(startTime) && currentTime.isBefore(endTime)) ||
        currentTime.isEqual(startTime) ||
        currentTime.isEqual(endTime);
  }

  private void resetConnectionsQuietly() {
    try {
      resetDBConnectionsIfRequired();
    } catch (SQLException sqlEx) {
      LOG.error("Error while connecting to DB {}", sqlEx);
      addToStageExceptionsQueue(new StageException(JDBC_00, sqlEx));
    } catch (StageException stageException) {
      addToStageExceptionsQueue(stageException);
    }
  }

  private void removeProcessedRecords(HashQueue<RecordSequence> records, int sequenceNumber) {
    Iterator<RecordSequence> recordSequenceIterator = records.iterator();
    while (recordSequenceIterator.hasNext() && recordSequenceIterator.next().seq <= sequenceNumber) {
      recordSequenceIterator.remove();
    }
  }

  private LocalDateTime adjustStartTime(LocalDateTime startTime) {
    return useLocalBuffering ? startTime : startTime.minusSeconds(configBean.txnWindow);
  }

  private Record generateRecord(
      String sql, Map<String, String> attributes, int operationCode
  ) throws UnparseableSQLException, StageException {
    SchemaAndTable table = new SchemaAndTable(attributes.get(SCHEMA), attributes.get(TABLE));
    int sdcOperationType = ParseUtil.getOperation(operationCode);
    attributes.put(OperationType.SDC_OPERATION_TYPE, String.valueOf(sdcOperationType));
    String id = useLocalBuffering
                ? attributes.get(RS_ID) + OFFSET_DELIM + attributes.get(SSN)
                : attributes.get(SCN) + OFFSET_DELIM + attributes.get(SEQ);
    Record record = getContext().createRecord(id);
    if (configBean.parseQuery) {
      Map<String, String> columns;
      if (configBean.useNewParser) {
        Set<String> columnsExpected = null;
        if (configBean.allowNulls && table.isNotEmpty()) {
          columnsExpected = tableSchemas.get(table).keySet();
        }
        columns = SQLParserUtils.process(sqlParser.get(),
            sql,
            operationCode,
            configBean.allowNulls,
            configBean.baseConfigBean.caseSensitive,
            columnsExpected
        );
      } else {
        // Walk it and attach our sqlListener
        sqlListener.get().reset();
        if (configBean.baseConfigBean.caseSensitive) {
          sqlListener.get().setCaseSensitive();
        }

        if (configBean.allowNulls) {
          sqlListener.get().allowNulls();
        }
        if (configBean.allowNulls && table.isNotEmpty()) {
          sqlListener.get().setColumns(tableSchemas.get(table).keySet());
        }

        parseTreeWalker.get().walk(sqlListener.get(), ParseUtil.getParserRuleContext(sql, operationCode));
        columns = sqlListener.get().getColumns();
      }

      String rowId = columns.get(ROWID);
      columns.remove(ROWID);
      if (rowId != null) {
        attributes.put(ROWID_KEY, rowId);
      }
      Map<String, Field> fields = new HashMap<>();

      List<UnsupportedFieldTypeException> fieldTypeExceptions = new ArrayList<>();
      for (Map.Entry<String, String> column : columns.entrySet()) {
        String columnName = column.getKey();
        Field createdField = null;
        try {
          createdField = objectToField(table, columnName, column.getValue());
        } catch (UnsupportedFieldTypeException ex) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Unsupported field type exception", ex);
          }
          if (configBean.sendUnsupportedFields) {
            createdField = Field.create(column.getValue());
          }
          fieldTypeExceptions.add(ex);
        }
        if (createdField != null) {
          if (decimalColumns.containsKey(table) && decimalColumns.get(table).containsKey(columnName)) {
            String precision = String.valueOf(decimalColumns.get(table).get(columnName).precision);
            String scale = String.valueOf(decimalColumns.get(table).get(columnName).scale);
            attributes.put("jdbc." + columnName + ".precision", precision);
            attributes.put("jdbc." + columnName + ".scale", scale);
            createdField.setAttribute(HeaderAttributeConstants.ATTR_PRECISION, precision);
            createdField.setAttribute(HeaderAttributeConstants.ATTR_SCALE, scale);
          }
          fields.put(columnName, createdField);
        }
      }
      record.set(Field.create(fields));
      attributes.forEach((k, v) -> record.getHeader().setAttribute(k, v));

      Joiner errorStringJoiner = Joiner.on(", ");
      List<String> errorColumns = Collections.emptyList();
      if (!fieldTypeExceptions.isEmpty()) {
        errorColumns = fieldTypeExceptions.stream().map(ex -> {
          String fieldTypeName = JDBCTypeNames.getOrDefault(ex.fieldType, "unknown");
          return "[Column = '" + ex.column + "', Type = '" + fieldTypeName + "', Value = '" + ex.columnVal + "']";
        }).collect(Collectors.toList());
      }
      if (!fieldTypeExceptions.isEmpty()) {
        boolean add = handleUnsupportedFieldTypes(record, errorStringJoiner.join(errorColumns));
        if (add) {
          return record;
        } else {
          return null;
        }
      } else {
        return record;
      }
    } else {
      attributes.forEach((k, v) -> record.getHeader().setAttribute(k, v));
      Map<String, Field> fields = new HashMap<>();
      fields.put("sql", Field.create(sql));
      record.set(Field.create(fields));
      return record;
    }

  }

  private boolean handleUnsupportedFieldTypes(Record r, String error) {
    switch (configBean.unsupportedFieldOp) {
      case SEND_TO_PIPELINE:
        if (LOG.isDebugEnabled()) {
          LOG.debug(UNSUPPORTED_SEND_TO_PIPELINE, error, r.getHeader().getAttribute(TABLE));
        }
        return true;
      case TO_ERROR:
        if (LOG.isDebugEnabled()) {
          LOG.debug(UNSUPPORTED_TO_ERR, error, r.getHeader().getAttribute(TABLE));
        }
        errorRecords.offer(new RecordErrorString(r, error));
        return false;
      case DISCARD:
        if (LOG.isDebugEnabled()) {
          LOG.debug(UNSUPPORTED_DISCARD, error, r.getHeader().getAttribute(TABLE));
        }
        return false;
      default:
        throw new IllegalStateException("Unknown Record Handling option");
    }
  }

  private long localDateTimeToEpoch(LocalDateTime date) {
    return date.atZone(zoneId).toEpochSecond();
  }

  private int addRecordsToQueueOLD(
      LocalDateTime commitTimestamp, String commitScn, String xid
  ) throws InterruptedException {
    TransactionIdKey key = new TransactionIdKey(xid);
    int seq = 0;
    bufferedRecordsLock.lock();
    HashQueue<RecordSequence> records;
    try {
      records = bufferedRecords.getOrDefault(key, EMPTY_LINKED_HASHSET);
      records.completeInserts();
      bufferedRecords.remove(key);
    } finally {
      bufferedRecordsLock.unlock();
    }
    final List<FutureWrapper> parseFutures = new ArrayList<>();
    while (!records.isEmpty()) {
      RecordSequence r = records.remove();
      if (configBean.keepOriginalQuery) {
        r.headers.put(QUERY_KEY, r.sqlString);
      }
      final Future<Record> recordFuture = parsingExecutor.submit(() -> generateRecord(
          r.sqlString,
          r.headers,
          r.opCode
      ));
      parseFutures.add(new FutureWrapper(recordFuture, r.sqlString, r.seq));
    }
    records.close();
    LinkedList<RecordOffset> recordOffsets = new LinkedList<>();
    for (FutureWrapper recordFuture : parseFutures) {
      try {
        Record record = recordFuture.future.get();
        if (record != null) {
          final RecordOffset recordOffset = new RecordOffset(
              record,
              new Offset(VERSION_UNCOMMITTED, commitTimestamp, commitScn, recordFuture.seq, xid)
          );

          // Is this a record generated by a rollback? If it is find the previous record that matches this row id and
          // remove it from the queue.
          if (recordOffset.record.getHeader().getAttribute(ROLLBACK).equals(ONE)) {
            String rowId = recordOffset.record.getHeader().getAttribute(ROWID_KEY);
            Iterator<RecordOffset> reverseIter = recordOffsets.descendingIterator();
            while (reverseIter.hasNext()) {
              if (reverseIter.next().record.getHeader().getAttribute(ROWID_KEY).equals(rowId)) {
                reverseIter.remove();
                break;
              }
            }
          } else {
            recordOffsets.add(recordOffset);
          }
        }
      } catch (ExecutionException e) {
        LOG.error("{}:{}", e.getMessage(), e);
        final Throwable cause = e.getCause();
        if (cause instanceof UnparseableSQLException) {
          unparseable.offer(recordFuture.sql);
        } else {
          otherErrors.offer(new ErrorAndCause(JDBC_405, cause));
        }
      }
    }

    for (RecordOffset ro : recordOffsets) {
      try {
        seq = ro.offset.sequence;
        while (!recordQueue.offer(ro, 1, TimeUnit.SECONDS)) {
          if (getContext().isStopped()) {
            return seq;
          }
        }
        LOG.debug(GENERATED_RECORD, ro.record, ro.record.getHeader().getAttribute(XID));
      } catch (InterruptedException ex) {
        try {
          errorRecordHandler.onError(JDBC_405, ex);
        } catch (StageException stageException) {
          addToStageExceptionsQueue(stageException);
        }
      }
    }
    return seq;
  }

  private int addRecordsToQueue(
      LocalDateTime commitTimestamp, String commitScn, String xid
  ) throws InterruptedException {

    TransactionIdKey key = new TransactionIdKey(xid);
    int seq = 0;

    HashQueue<RecordSequence> records;

    bufferedRecordsLock.lock();
    try {
      records = bufferedRecords.getOrDefault(key, EMPTY_LINKED_HASHSET);
      records.completeInserts();
      bufferedRecords.remove(key);
    } finally {
      bufferedRecordsLock.unlock();
    }

    final List<FutureWrapper> parseFutures = new LinkedList<>();
    while (!records.isEmpty()) {
      parseFutures.clear();

      for (int i = 0; i < 2 * configBean.baseConfigBean.maxBatchSize; i++) {
        if (records.isEmpty()) {
          break;
        }
        RecordSequence r = records.remove();

        // Check this record to determine if it's affected by a 'ROLLBACK TO'.
        // in this case we do not want to let the affected record go into the SDC Batch...
        if (r.headers.get(SKIP) != null) {
          LOG.debug("addRecordsToQueue(): skipping record due to 'ROLLBACK TO': {} rowId: '{}'",
              r.sqlString,
              r.headers.get(ROWID_KEY)
          );
          continue;
        }

        if (configBean.keepOriginalQuery) {
          r.headers.put(QUERY_KEY, r.sqlString);
        }

        try {
          final Future<Record> recordFuture = parsingExecutor.submit(() -> generateRecord(r.sqlString,
              r.headers,
              r.opCode
          ));
          parseFutures.add(new FutureWrapper(recordFuture, r.sqlString, r.seq));

        } catch (Exception ex) {
          LOG.error("Exception {}", ex.getMessage(), ex);
          final Throwable cause = ex.getCause();
          otherErrors.offer(new ErrorAndCause(JDBC_405, cause));
        }
      }

      for (FutureWrapper recordFuture : parseFutures) {
        try {
          final RecordOffset recordOffset;
          Record record = recordFuture.future.get();
          recordOffset = new RecordOffset(record,
              new Offset(VERSION_UNCOMMITTED, commitTimestamp, commitScn, recordFuture.seq, xid)
          );

          seq = recordOffset.offset.sequence;

          while (!recordQueue.offer(recordOffset, 1, TimeUnit.SECONDS)) {
            if (getContext().isStopped()) {
              records.close();
              return seq;
            }
          }

          LOG.trace(GENERATED_RECORD, recordOffset.record, recordOffset.record.getHeader().getAttribute(XID));
        } catch (InterruptedException | ExecutionException ex) {
          try {
            errorRecordHandler.onError(JDBC_405, ex);
          } catch (StageException stageException) {
            addToStageExceptionsQueue(stageException);
          }
        }
      }
    }

    records.close();
    return seq;
  }

  private EventRecord createEventRecord(
      DDL_EVENT type, String redoSQL, SchemaAndTable schemaAndTable, String scnSeq, boolean sendSchema, String timestamp
  ) {
    EventRecord event = getContext().createEventRecord(type.name(), 1, scnSeq);
    event.getHeader().setAttribute(TABLE, schemaAndTable.getTable());
    if (redoSQL != null) {
      event.getHeader().setAttribute(DDL_TEXT, redoSQL);
    }
    if (sendSchema) {
      // Note that the schema inserted is the *current* schema and not the result of the DDL.
      // Getting the schema as a result of the DDL is not possible.
      // We actually don't know the schema at table creation ever, but just the schema when we started. So
      // trying to figure out the schema at the time of the DDL is not really possible since this DDL could have occured
      // before the source started. Since we allow only types to be bigger and no column drops, this is ok.
      Map<String, Integer> schema = tableSchemas.get(schemaAndTable);
      Map<String, Field> fields = new HashMap<>();
      for (Map.Entry<String, Integer> column : schema.entrySet()) {
        fields.put(column.getKey(), Field.create(JDBCTypeNames.get(column.getValue())));
      }
      event.set(Field.create(fields));
    }
    if (timestamp != null) {
      event.getHeader().setAttribute(TIMESTAMP_HEADER, timestamp);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Event produced: {}", event);
    }
    return event;
  }

  private DDL_EVENT getDdlType(String redoSQL) {
    DDL_EVENT ddlType;
    try {
      Matcher ddlMatcher = ddlPattern.matcher(redoSQL.toUpperCase());
      if (!ddlMatcher.find()) {
        ddlType = DDL_EVENT.UNKNOWN;
      } else {
        ddlType = DDL_EVENT.valueOf(ddlMatcher.group(1));
      }
    } catch (IllegalArgumentException e) {
      LOG.warn("Unknown DDL Type for statement: " + redoSQL, e);
      ddlType = DDL_EVENT.UNKNOWN;
    }
    return ddlType;
  }

  /**
   * Refresh the schema for the table if the last update of this table was before the given SCN.
   * Returns true if it was updated, else returns false.
   */
  private boolean refreshSchema(BigDecimal scnDecimal, SchemaAndTable schemaAndTable) throws SQLException {
    try {
      if (!tableSchemaLastUpdate.containsKey(schemaAndTable) || scnDecimal.compareTo(tableSchemaLastUpdate.get(
          schemaAndTable)) > 0) {
        if (containerized) {
          try (Statement switchToPdb = connection.createStatement()) {
            switchToPdb.execute("ALTER SESSION SET CONTAINER = " + configBean.pdb);
          }
        }
        tableSchemas.put(schemaAndTable, getTableSchema(schemaAndTable));
        tableSchemaLastUpdate.put(schemaAndTable, scnDecimal);
        return true;
      }
      return false;
    } finally {
      alterSession();
    }
  }

  private void addToStageExceptionsQueue(StageException ex) {
    try {
      while (!stageExceptions.offer(ex, 1, TimeUnit.SECONDS) && !getContext().isStopped()) {
        //Empty while
      }
    } catch (InterruptedException iEx) {
      LOG.error("Interrupted while adding stage exception to queue");
      Thread.currentThread().interrupt();
    }
  }

  private LocalDateTime getDateForSCN(BigDecimal commitSCN) throws SQLException {
    startLogMinerUsingGivenSCNs(commitSCN, getEndingSCN());
    getTimestampsFromLogMnrContents.setMaxRows(1);
    try (ResultSet rs = getTimestampsFromLogMnrContents.executeQuery()) {
      if (rs.next()) {
        LocalDateTime date = rs.getTimestamp(1).toLocalDateTime();
        LOG.debug(START_DATE_REFRESHED_TO, date);
        return date;
      }
    }
    throw new IllegalStateException(Utils.format(
        "SCN: '{}' is not valid and cannot be found in LogMiner logs",
        commitSCN.toPlainString()
    ));
  }

  private LocalDateTime getEndTimeForStartTime(LocalDateTime startTime) {
    return startTime.plusSeconds(configBean.logminerWindow);
  }

  private void startLogMinerUsingGivenSCNs(BigDecimal oldestSCN, BigDecimal endSCN) throws SQLException {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug(TRYING_TO_START_LOG_MINER_WITH_START_SCN_AND_END_SCN,
            oldestSCN.toPlainString(),
            endSCN.toPlainString()
        );
      }
      startLogMnrForCommitSCN.setBigDecimal(1, oldestSCN);
      startLogMnrForCommitSCN.setBigDecimal(2, endSCN);
      startLogMnrForCommitSCN.execute();
      if (LOG.isDebugEnabled()) {
        LOG.debug(STARTED_LOG_MINER_WITH_START_SCN_AND_END_SCN, oldestSCN.toPlainString(), endSCN.toPlainString());
      }
    } catch (SQLException ex) {
      LOG.debug("SQLException while starting LogMiner", ex);
      throw ex;
    }
  }

  private void startLogMinerUsingGivenDates(String startDate, String endDate) throws SQLException, StageException {
    try {
      startLogMnrForRedoDict();
      LOG.info(TRYING_TO_START_LOG_MINER_WITH_START_DATE_AND_END_DATE, startDate, endDate);
      startLogMnrForData.setString(1, startDate);
      startLogMnrForData.setString(2, endDate);
      startLogMnrForData.execute();
    } catch (SQLException ex) {
      LOG.debug("SQLException while starting LogMiner", ex);
      resetConnectionsQuietly();
      throw ex;
    }
  }

  private BasicConnectionString getBasicConnectionString() {
    return new BasicConnectionString(hikariConfigBean.getPatterns(), hikariConfigBean.getConnectionStringTemplate());
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    // save configuration parameter
    useNewAddRecordsToQueue = getContext().getConfiguration().get(CONFIG_PROPERTY, CONFIG_PROPERTY_DEFAULT_VALUE);
    LOG.info("init():  {} = {} ", CONFIG_PROPERTY, useNewAddRecordsToQueue);

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    useLocalBuffering = !getContext().isPreview() && configBean.bufferLocally;

    sshTunnelService = getContext().getService(SshTunnelService.class);

    BasicConnectionString.Info
        info
        = getBasicConnectionString().getBasicConnectionInfo(hikariConfigBean.getConnectionString());

    if (info != null) {
      // basic connection string format
      SshTunnelService.HostPort target = new SshTunnelService.HostPort(info.getHost(), info.getPort());
      Map<SshTunnelService.HostPort, SshTunnelService.HostPort>
          portMapping
          = sshTunnelService.start(Collections.singletonList(target));
      SshTunnelService.HostPort tunnel = portMapping.get(target);
      info = info.changeHostPort(tunnel.getHost(), tunnel.getPort());
      hikariConfigBean.setConnectionString(getBasicConnectionString().getBasicConnectionUrl(info));
    } else {
      // complex connection string format, we don't support this right now with SSH tunneling
      issues.add(getContext().createConfigIssue("JDBC",
          "hikariConfigBean.connectionString",
          hikariConfigBean.getNonBasicUrlErrorCode()
      ));
    }

    if (!issues.isEmpty()) {
      return issues;
    }

    issues = hikariConfigBean.validateConfigs(getContext(), issues);
    if (connection == null) { // For tests, we set a mock connection
      try {
        dataSource = jdbcUtil.createDataSourceForRead(hikariConfigBean);
        connection = dataSource.getConnection();
        connection.setAutoCommit(false);
      } catch (StageException e) {
        LOG.error("Error while connecting to DB", e);
        issues.add(getContext().createConfigIssue(Groups.JDBC.name(), CONNECTION_STR, e.getErrorCode(), e.getParams()));
        return issues;
      } catch (SQLException e) {
        LOG.error("Error while connecting to DB", e);
        issues.add(getContext().createConfigIssue(Groups.JDBC.name(), CONNECTION_STR, JDBC_00, e.toString()));
        return issues;
      }
    }

    recordQueue = new LinkedBlockingQueue<>(2 * configBean.baseConfigBean.maxBatchSize);
    String container = configBean.pdb;

    List<SchemaAndTable> schemasAndTables;

    try {
      initializeStatements();
      alterSession();
    } catch (SQLException ex) {
      LOG.error("Error while creating statement", ex);
      issues.add(getContext().createConfigIssue(Groups.JDBC.name(),
          CONNECTION_STR,
          JDBC_00,
          hikariConfigBean.getConnectionString()
      ));
    }
    zoneId = ZoneId.of(configBean.dbTimeZone);
    dateTimeColumnHandler = new DateTimeColumnHandler(zoneId, configBean.convertTimestampToString);
    String commitScnField;
    BigDecimal scn = null;
    try {
      scn = getEndingSCN();
      switch (configBean.startValue) {
        case SCN:
          if (new BigDecimal(configBean.startSCN).compareTo(scn) > 0) {
            issues.add(getContext().createConfigIssue(
                CDC.name(),
                "oracleCDCConfigBean.startSCN",
                JDBC_47,
                scn.toPlainString()
            ));
          }
          break;
        case LATEST:
          // If LATEST is used, use now() as the startDate and proceed as if a startDate was specified
          configBean.startDate = nowAtDBTz().format(dateTimeColumnHandler.dateFormatter);
          // fall-through
        case DATE:
          try {
            LocalDateTime startDate = dateTimeColumnHandler.getDate(configBean.startDate);
            if (startDate.isAfter(nowAtDBTz())) {
              issues.add(getContext().createConfigIssue(CDC.name(), "oracleCDCConfigBean.startDate", JDBC_48));
            }
          } catch (DateTimeParseException ex) {
            LOG.error("Invalid date", ex);
            issues.add(getContext().createConfigIssue(CDC.name(), "oracleCDCConfigBean.startDate", JDBC_49));
          }
          break;
        default:
          throw new IllegalStateException("Unknown start value!");
      }
    } catch (SQLException ex) {
      LOG.error("Error while getting SCN", ex);
      issues.add(getContext().createConfigIssue(CREDENTIALS.name(), USERNAME, JDBC_42));
    }

    try (Statement reusedStatement = connection.createStatement()) {
      int majorVersion = getDBVersion(issues);
      // If version is 12+, then the check for table presence must be done in an alternate container!
      if (majorVersion == -1) {
        return issues;
      }
      if (majorVersion >= 12) {
        if (!StringUtils.isEmpty(container)) {
          String switchToPdb = "ALTER SESSION SET CONTAINER = " + configBean.pdb;
          try {
            reusedStatement.execute(switchToPdb);
          } catch (SQLException ex) {
            LOG.error("Error while switching to container: " + container, ex);
            issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(), USERNAME, JDBC_40, container));
            return issues;
          }
          containerized = true;
        }
      }

      schemasAndTables = new ArrayList<>();
      for (SchemaTableConfigBean tables : configBean.baseConfigBean.schemaTableConfigs) {

        tables.schema = configBean.baseConfigBean.caseSensitive ? tables.schema : tables.schema.toUpperCase();
        tables.table = configBean.baseConfigBean.caseSensitive ? tables.table : tables.table.toUpperCase();
        if (tables.excludePattern != null) {
          tables.excludePattern = configBean.baseConfigBean.caseSensitive
                                  ? tables.excludePattern
                                  : tables.excludePattern.toUpperCase();
        }
        Pattern p = StringUtils.isEmpty(tables.excludePattern) ? null : Pattern.compile(tables.excludePattern);

        try (ResultSet rs = jdbcUtil.getTableAndViewMetadata(connection, tables.schema, tables.table)) {
          while (rs.next()) {
            String schemaName = rs.getString(TABLE_METADATA_TABLE_SCHEMA_CONSTANT);
            String tableName = rs.getString(TABLE_METADATA_TABLE_NAME_CONSTANT);
            if (p == null || !p.matcher(tableName).matches()) {
              schemaName = schemaName.trim();
              tableName = tableName.trim();
              schemasAndTables.add(new SchemaAndTable(schemaName, tableName));
            }
          }
        }
      }

      validateTablePresence(reusedStatement, schemasAndTables, issues);
      if (!issues.isEmpty()) {
        return issues;
      }
      for (SchemaAndTable schemaAndTable : schemasAndTables) {
        try {
          tableSchemas.put(schemaAndTable, getTableSchema(schemaAndTable));
          if (scn != null) {
            tableSchemaLastUpdate.put(schemaAndTable, scn);
          }
        } catch (SQLException ex) {
          LOG.error("Error while switching to container: " + container, ex);
          issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(), USERNAME, JDBC_50));
        }
      }
      container = CDB_ROOT;
      if (majorVersion >= 12) {
        try {
          switchContainer.execute();
          LOG.info("Switched to CDB$ROOT to start LogMiner.");
        } catch (SQLException ex) {
          // Fatal only if we switched to a PDB earlier
          if (containerized) {
            LOG.error("Error while switching to container: " + container, ex);
            issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(), USERNAME, JDBC_40, container));
            return issues;
          }
          // Log it anyway
          LOG.info("Switching containers failed, ignoring since there was no PDB switch", ex);
        }
      }
      commitScnField = majorVersion >= 11 ? "COMMIT_SCN" : "CSCN";
    } catch (SQLException ex) {
      LOG.error("Error while creating statement", ex);
      issues.add(getContext().createConfigIssue(Groups.JDBC.name(),
          CONNECTION_STR,
          JDBC_00,
          hikariConfigBean.getConnectionString()
      ));
      return issues;
    }

    final String ddlTracking = shouldTrackDDL ? " + DBMS_LOGMNR.DDL_DICT_TRACKING" : "";

    final String readCommitted = useLocalBuffering ? "" : "+ DBMS_LOGMNR.COMMITTED_DATA_ONLY";

    this.logMinerProcedure = "BEGIN" +
        " DBMS_LOGMNR.START_LOGMNR(" +
        " {}," +
        " {}," +
        " OPTIONS => DBMS_LOGMNR." +
        configBean.dictionary.name() +
        "          + DBMS_LOGMNR.CONTINUOUS_MINE" +
        readCommitted +
        "          + DBMS_LOGMNR.NO_SQL_DELIMITER" +
        ddlTracking +
        ");" +
        " END;";

    final String base = "SELECT SCN, USERNAME, OPERATION_CODE, TIMESTAMP, SQL_REDO, TABLE_NAME, " +
        commitScnField +
        ", SEQUENCE#, CSF, XIDUSN, XIDSLT, XIDSQN, RS_ID, SSN, SEG_OWNER, ROLLBACK, ROW_ID " +
        " FROM V$LOGMNR_CONTENTS" +
        " WHERE ";

    final String tableCondition = getListOfSchemasAndTables(schemasAndTables);

    final String commitRollbackCondition = Utils.format("OPERATION_CODE = {} OR OPERATION_CODE = {}",
        COMMIT_CODE,
        ROLLBACK_CODE
    );

    final String operationsCondition = "OPERATION_CODE IN (" + getSupportedOperations() + ")";

    final String restartNonBufferCondition = Utils.format("((" +
        commitScnField +
        " = ? AND SEQUENCE# > ?) OR " +
        commitScnField +
        "  > ?)" +
        (shouldTrackDDL ? " OR (OPERATION_CODE = {} AND SCN > ?)" : ""), DDL_CODE);


    if (useLocalBuffering) {
      selectString = String.format(
          "%s ((%s AND (%s)) OR (%s))",
          base,
          tableCondition,
          operationsCondition,
          commitRollbackCondition
      );
    } else {
      selectString = base +
          " (" +
          tableCondition +
          " AND (" +
          operationsCondition +
          "))" +
          "AND (" +
          restartNonBufferCondition +
          ")";
    }

    try {
      initializeLogMnrStatements();
    } catch (SQLException ex) {
      LOG.error("Error while creating statement", ex);
      issues.add(getContext().createConfigIssue(Groups.JDBC.name(),
          CONNECTION_STR,
          JDBC_00,
          hikariConfigBean.getConnectionString()
      ));
    }

    if (configBean.dictionary == DictionaryValues.DICT_FROM_REDO_LOGS) {
      try {
        startLogMnrForRedoDict();
      } catch (Exception ex) {
        LOG.warn("Error while attempting to start LogMiner to load dictionary", ex);
        issues.add(getContext().createConfigIssue(Groups.CDC.name(), "oracleCDCConfigBean.dictionary", JDBC_44, ex));
      }
    }

    if (useLocalBuffering && configBean.bufferLocation == BufferingValues.ON_DISK) {
      File tmpDir = new File(System.getProperty("java.io.tmpdir"));
      String relativePath = getContext().getSdcId() +
          "/" +
          getContext().getPipelineId() +
          "/" +
          getContext().getStageInfo().getInstanceName();
      this.txnBufferLocation = new File(tmpDir, relativePath);

      try {
        if (txnBufferLocation.exists()) {
          FileUtils.deleteDirectory(txnBufferLocation);
          LOG.info("Deleted " + txnBufferLocation.toString());
        }
        Files.createDirectories(txnBufferLocation.toPath());
        LOG.info("Created " + txnBufferLocation.toString());
      } catch (IOException ex) {
        Throwables.propagate(ex);
      }
    }

    if (configBean.bufferLocally) {
      if (configBean.parseQuery) {
        parsingExecutor = Executors.newFixedThreadPool(configBean.parseThreadPoolSize,
            new ThreadFactoryBuilder().setNameFormat("Oracle CDC Origin Parse Thread - %d").build()
        );
      } else {
        parsingExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(
            "Oracle CDC Origin Parse Thread - %d").build());
      }
    }

    if (configBean.txnWindow >= configBean.logminerWindow) {
      issues.add(getContext().createConfigIssue(Groups.CDC.name(), "oracleCDCConfigBean.logminerWindow", JDBC_81));
    }
    version = useLocalBuffering ? VERSION_UNCOMMITTED : VERSION_STR;
    delay = getContext().createGauge("Read Lag (seconds)");
    return issues;
  }

  /**
   * This method needs to get SQL like string with all required schemas and tables.
   *
   * @param schemaAndTables List of SchemaAndTable objects
   * @return SQL string of schemas and tables
   */
  @VisibleForTesting
  String getListOfSchemasAndTables(List<SchemaAndTable> schemaAndTables) {
    Map<String, List<String>> schemas = new HashMap<>();
    for (SchemaAndTable schemaAndTable : schemaAndTables) {
      if (schemas.containsKey(schemaAndTable.getSchema())) {
        schemas.get(schemaAndTable.getSchema()).add(schemaAndTable.getTable());
      } else {
        List<String> tbls = new ArrayList<>();
        tbls.add(schemaAndTable.getTable());
        schemas.put(schemaAndTable.getSchema(), tbls);
      }
    }
    List<String> queries = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : schemas.entrySet()) {
      List<String> tables = new ArrayList<>();
      int fromIndex = 0;
      int range = 1000;
      int maxIndex = entry.getValue().size();
      int toIndex = Math.min(range, maxIndex);
      while (fromIndex < toIndex) {
        tables.add(Utils.format("TABLE_NAME IN ({})", formatTableList(entry.getValue().subList(fromIndex, toIndex))));
        fromIndex = toIndex;
        toIndex = Math.min((toIndex + range), maxIndex);
      }
      queries.add(Utils.format("(SEG_OWNER='{}' AND ({}))", entry.getKey(), String.join(" OR ", tables)));
    }
    return "( " + String.join(" OR ", queries) + " )";
  }

  @NotNull
  private LocalDateTime nowAtDBTz() {
    return LocalDateTime.now(zoneId);
  }

  private void startLogMnrForRedoDict() throws SQLException, StageException {
    if (configBean.dictionary != DictionaryValues.DICT_FROM_REDO_LOGS) {
      return;
    }
    resetDBConnectionsIfRequired();
    BigDecimal endSCN = getEndingSCN();

    SQLException lastException = null;
    boolean startedLogMiner = false;

    if (cachedSCNForRedoLogs.get().compareTo(BigDecimal.ZERO) > 0) { // There is a cached SCN, let's try that one.
      try {
        startLogMinerUsingGivenSCNs(cachedSCNForRedoLogs.get(), endSCN);
        // Still valid, so return
        return;
      } catch (SQLException e) {
        LOG.debug("Cached SCN {} is no longer valid, retrieving new SCN", cachedSCNForRedoLogs);
      }
    }

    // Cached SCN is no longer valid, try to get the next oldest ones and start.
    getOldestSCN.setBigDecimal(1, cachedSCNForRedoLogs.get());
    try (ResultSet rs = getOldestSCN.executeQuery()) {
      while (rs.next()) {
        BigDecimal oldestSCN = rs.getBigDecimal(1);
        try {
          startLogMinerUsingGivenSCNs(oldestSCN, endSCN);
          cachedSCNForRedoLogs.set(oldestSCN);
          startedLogMiner = true;
          break;
        } catch (SQLException ex) {
          lastException = ex;
        }
      }
    }
    connection.commit();
    if (!startedLogMiner) {
      if (lastException != null) {
        throw new StageException(JDBC_52, lastException);
      } else {
        throw new StageException(JDBC_52);
      }
    }
  }

  private void initializeStatements() throws SQLException {
    getOldestSCN = connection.prepareStatement(GET_OLDEST_SCN);
    getLatestSCN = connection.prepareStatement(CURRENT_SCN);
    dateStatement = connection.prepareStatement(NLS_DATE_FORMAT);
    tsStatement = connection.prepareStatement(NLS_TIMESTAMP_FORMAT);
    tsTzStatement = connection.prepareStatement(NLS_TIMESTAMP_TZ_FORMAT);
    numericFormat = connection.prepareStatement(NLS_NUMERIC_FORMAT);
    switchContainer = connection.prepareStatement(SWITCH_TO_CDB_ROOT);
  }

  private void initializeLogMnrStatements() throws SQLException {
    selectFromLogMnrContents = getSelectChangesStatement();
    startLogMnrForCommitSCN = connection.prepareCall(Utils.format(logMinerProcedure, "STARTSCN => ?", "ENDSCN => ?"));
    startLogMnrForData = connection.prepareCall(Utils.format(logMinerProcedure, "STARTTIME => ?", "ENDTIME => ?"));
    startLogMnrSCNToDate = connection.prepareCall(Utils.format(logMinerProcedure, "STARTSCN => ?", "ENDTIME => ?"));
    endLogMnr = connection.prepareCall("BEGIN DBMS_LOGMNR.END_LOGMNR; END;");
    getTimestampsFromLogMnrContents = connection.prepareStatement(GET_TIMESTAMPS_FROM_LOGMNR_CONTENTS);
    LOG.debug(REDO_SELECT_QUERY, selectString);
  }

  private PreparedStatement getSelectChangesStatement() throws SQLException {
    return connection.prepareStatement(selectString);
  }

  private String formatTableList(List<String> tables) {
    List<String> quoted = new ArrayList<>();
    for (String table : tables) {
      quoted.add(String.format("'%s'", table));
    }
    Joiner joiner = Joiner.on(",");
    return joiner.join(quoted);
  }

  private String getSupportedOperations() {
    List<Integer> supportedOps = new ArrayList<>();

    for (ChangeTypeValues change : configBean.baseConfigBean.changeTypes) {
      switch (change) {
        case INSERT:
          supportedOps.add(INSERT_CODE);
          break;
        case UPDATE:
          supportedOps.add(UPDATE_CODE);
          break;
        case DELETE:
          supportedOps.add(DELETE_CODE);
          break;
        case SELECT_FOR_UPDATE:
          supportedOps.add(SELECT_FOR_UPDATE_CODE);
          break;
        default:
      }
    }
    if (shouldTrackDDL) {
      supportedOps.add(DDL_CODE);
    }
    Joiner joiner = Joiner.on(',');
    return joiner.join(supportedOps);
  }

  private BigDecimal getEndingSCN() throws SQLException {
    try (ResultSet rs = getLatestSCN.executeQuery()) {
      if (!rs.next()) {
        throw new SQLException("Missing SCN");
      }
      BigDecimal scn = rs.getBigDecimal(1);
      if (LOG.isDebugEnabled()) {
        LOG.debug(CURRENT_LATEST_SCN_IS, scn.toPlainString());
      }
      return scn;
    }
  }

  private void validateTablePresence(
      Statement statement, List<SchemaAndTable> schemaAndTables, List<ConfigIssue> issues
  ) {
    if (schemaAndTables.isEmpty()) {
      LOG.error(JDBC_404.getMessage());
      issues.add(getContext().createConfigIssue(Groups.CDC.name(),
          "oracleCDCConfigBean.baseConfigBean.tables",
          JDBC_404
      ));
    }
    for (SchemaAndTable schemaAndTable : schemaAndTables) {
      LOG.info("Found table: '{}' in schema: '{}'", schemaAndTable.getTable(), schemaAndTable.getSchema());
      try {
        statement.execute("SELECT * FROM \"" +
            schemaAndTable.getSchema() +
            "\".\"" +
            schemaAndTable.getTable() +
            "\" WHERE 1 = 0");
      } catch (SQLException ex) {
        StringBuilder sb = new StringBuilder("Table: ").append(schemaAndTable).append(" does not exist.");
        if (StringUtils.isEmpty(configBean.pdb)) {
          sb.append(" PDB was not specified. If the database was created inside a PDB, please specify PDB");
        }
        LOG.error(sb.toString(), ex);
        issues.add(getContext().createConfigIssue(Groups.CDC.name(),
            "oracleCDCConfigBean.baseConfigBean.tables",
            JDBC_16,
            schemaAndTable
        ));
      }
    }
  }

  private Map<String, Integer> getTableSchema(SchemaAndTable schemaAndTable) throws SQLException {
    Map<String, Integer> columns = new HashMap<>();
    String query = "SELECT * FROM \"" +
        schemaAndTable.getSchema() +
        "\".\"" +
        schemaAndTable.getTable() +
        "\" WHERE 1 = 0";
    try (Statement schemaStatement = connection.createStatement(); ResultSet rs = schemaStatement.executeQuery(query)) {
      ResultSetMetaData md = rs.getMetaData();
      int colCount = md.getColumnCount();
      for (int i = 1; i <= colCount; i++) {
        int colType = md.getColumnType(i);
        String colName = md.getColumnName(i);
        if (!configBean.baseConfigBean.caseSensitive) {
          colName = colName.toUpperCase();
        }
        if (colType == Types.DATE || colType == Types.TIME || colType == Types.TIMESTAMP) {
          dateTimeColumns.computeIfAbsent(schemaAndTable, k -> new HashMap<>());
          dateTimeColumns.get(schemaAndTable).put(colName, md.getColumnTypeName(i));
        }

        if (colType == Types.DECIMAL || colType == Types.NUMERIC) {
          decimalColumns.computeIfAbsent(schemaAndTable, k -> new HashMap<>());
          decimalColumns.get(schemaAndTable).put(colName, new PrecisionAndScale(md.getPrecision(i), md.getScale(i)));
        }
        columns.put(md.getColumnName(i), md.getColumnType(i));
      }
    }
    return columns;
  }

  private int getDBVersion(List<ConfigIssue> issues) {
    // Getting metadata version using connection.getMetaData().getDatabaseProductVersion() returns 12c which makes
    // comparisons brittle, so use the actual numerical versions.
    try (Statement statement = connection.createStatement(); ResultSet versionSet = statement.executeQuery(
        "SELECT version FROM product_component_version")) {
      if (versionSet.next()) {
        String versionStr = versionSet.getString("version");
        if (versionStr != null) {
          int majorVersion = Integer.parseInt(versionStr.substring(0, versionStr.indexOf('.')));
          LOG.info("Oracle Version is " + majorVersion);
          return majorVersion;
        }
      }
    } catch (SQLException ex) {
      LOG.error("Error while getting db version info", ex);
      issues.add(getContext().createConfigIssue(Groups.JDBC.name(), CONNECTION_STR, JDBC_41));
    }
    return -1;
  }

  @Override
  public void destroy() {
    //We get into a destroy after the batch has been completed.
    generationExecutor.shutdown();
    try {
      //Giving 30 seconds for the thread to complete or else send interrupt
      generationExecutor.awaitTermination(30, TimeUnit.SECONDS);
      if (!generationExecutor.isTerminated()) {
        generationExecutor.shutdownNow();
      }
    } catch (InterruptedException ex) {
      LOG.error("Interrupted while attempting to shutdown Generator thread", ex);
      Thread.currentThread().interrupt();
    }
    try {
      if (endLogMnr != null && !endLogMnr.isClosed()) {
        endLogMnr.execute();
      }
    } catch (SQLException ex) {
      LOG.warn("Error while stopping LogMiner", ex);
    }

    // Close all statements
    try {
      closeAllStatements();
    } catch (Exception ex) {
      LOG.error("Error while attempting to close statements", ex);
    }

    // Connection if it exists
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException ex) {
      LOG.warn("Error while closing connection to database", ex);
    }

    // And finally the hiraki data source
    if (dataSource != null) {
      dataSource.close();
    }

    bufferedRecordsLock.lock();
    try {
      this.bufferedRecords.forEach((x, y) -> y.close());
    } finally {
      bufferedRecordsLock.unlock();
    }
    generationStarted = false;

    if (sshTunnelService != null && getContext().getService(SshTunnelService.class).isEnabled()) {
      sshTunnelService.stop();
    }

    if (parsingExecutor != null) {
      parsingExecutor.shutdown();
    }
  }

  private void closeAllStatements() throws Exception {
    java.lang.reflect.Field[] fields = this.getClass().getFields();
    Method m = Statement.class.getMethod("close");
    for (java.lang.reflect.Field field : fields) {
      if (Statement.class.isAssignableFrom(field.getClass())) {
        field.setAccessible(true);
        try {
          m.invoke(field.get(this));
        } catch (Exception ex) {
          LOG.error("Error while closing statement!", ex);
        }
      }
    }

  }

  private Field objectToField(SchemaAndTable schemaAndTable, String column, String columnValue) throws StageException {
    Map<String, Integer> tableSchema = tableSchemas.get(schemaAndTable);
    if (!tableSchema.containsKey(column)) {
      throw new StageException(JDBC_54, column, schemaAndTable);
    }
    int columnType = tableSchema.get(column);

    return ParseUtil.generateField(column, columnValue, columnType, dateTimeColumnHandler);
  }


  private void alterSession() throws SQLException {
    if (containerized) {
      switchContainer.execute();
    }
    dateStatement.execute();
    tsStatement.execute();
    numericFormat.execute();
    tsTzStatement.execute();
  }

  private void discardOldUncommitted(LocalDateTime startTime) {
    if (!useLocalBuffering) {
      return;
    }
    bufferedRecordsLock.lock();
    try {
      AtomicInteger txnDiscarded = new AtomicInteger(0);
      AtomicInteger recordsDiscarded = new AtomicInteger(0);
      LOG.info("Removing expired transactions.");
      Iterator<Map.Entry<TransactionIdKey, HashQueue<RecordSequence>>> iter = bufferedRecords.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<TransactionIdKey, HashQueue<RecordSequence>> entry = iter.next();
        if (expired(entry, startTime)) {
          LOG.info("Removing transaction with id: " + entry.getKey().txnId);
          if (!configBean.discardExpired) {
            for (RecordSequence x : entry.getValue()) {
              try {
                Record record = generateRecord(x.sqlString, x.headers, x.opCode);
                if (record != null) {
                  expiredRecords.offer(new RecordTxnInfo(record, entry.getKey().txnId, entry.getKey().txnStartTime));
                }
              } catch (UnparseableSQLException ex) {
                unparseable.offer(x.sqlString);
              } catch (Exception ex) {
                LOG.error("Error while generating expired record from SQL: " + x.sqlString);
              }
              recordsDiscarded.incrementAndGet();
            }
          }
          txnDiscarded.incrementAndGet();
          iter.remove();
        }
      }
      LOG.info(Utils.format("Removed {} transactions and a total of {} records.",
          txnDiscarded.get(),
          recordsDiscarded.get()
      ));
    } finally {
      bufferedRecordsLock.unlock();
    }
  }

  /**
   * An element is "expired" if the transaction started before the current window being processed
   * and if no records have actually been sent to the pipeline. If a record has been sent, then a commit was seen,
   * so it is not expired.
   *
   * @param entry
   * @return
   */
  private boolean expired(Map.Entry<TransactionIdKey, HashQueue<RecordSequence>> entry, LocalDateTime startTime) {
    return startTime != null &&
        // Can be null if starting from SCN and first batch is not complete yet.
        entry.getKey().txnStartTime.isBefore(startTime.minusSeconds(configBean.txnWindow)) &&
        entry.getValue().peek().seq == 1;
  }

  @VisibleForTesting
  void setConnection(Connection conn) {
    this.connection = conn;
  }

  @VisibleForTesting
  void setDataSource(HikariDataSource dataSource) {
    this.dataSource = dataSource;
  }

  private HashQueue<RecordSequence> createTransactionBuffer(String txnId) {
    try {
      return configBean.bufferLocation == BufferingValues.IN_MEMORY
             ? new InMemoryHashQueue<>()
             : new FileBackedHashQueue<>(new File(txnBufferLocation, txnId));
    } catch (IOException ex) {
      LOG.error("Error while creating transaction buffer", ex);
      throw new RuntimeException(ex);
    }
  }

  private class TransactionIdKey {
    final String txnId;
    LocalDateTime txnStartTime;

    public TransactionIdKey(String txnId) {
      this.txnId = txnId;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof TransactionIdKey && this.txnId.equals(((TransactionIdKey) o).txnId);
    }

    @Override
    public int hashCode() {
      return txnId.hashCode();
    }

    public void setTxnStartTime(LocalDateTime time) {
      this.txnStartTime = time;
    }

  }

  private class Offset {
    final String version;
    LocalDateTime timestamp;
    final String scn;
    String txnId;

    final int sequence;

    public Offset(String version, LocalDateTime timestamp, String scn, int sequence, String txId) {
      this.version = version;
      this.scn = scn;
      this.timestamp = timestamp;
      this.sequence = sequence;
      this.txnId = (txId == null ? "" : txId);
    }

    public Offset(String offsetString) {
      Iterator<String> splits = Iterators.forArray(offsetString.split(OFFSET_DELIM));
      this.version = splits.next();
      this.timestamp = version.equals(VERSION_UNCOMMITTED)
                       ? LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(splits.next())), zoneId)
                       : null;
      this.scn = splits.next();
      this.sequence = Integer.parseInt(splits.next());
      if (splits.hasNext()) {
        this.txnId = splits.next();
      } else {
        this.txnId = "";
      }
    }

    public String toString() {
      List<String> parts = new ArrayList<>();
      parts.add(version);
      if ((version.equals(VERSION_UNCOMMITTED))) {
        parts.add(String.valueOf(localDateTimeToEpoch(timestamp)));
      }
      parts.add(scn);
      parts.add(String.valueOf(sequence));
      if (StringUtils.isNotEmpty(txnId)) {
        parts.add(txnId);
      }
      return Joiner.on(OFFSET_DELIM).join(parts);
    }
  }

  private class RecordOffset {
    final Record record;
    final Offset offset;

    public RecordOffset(Record record, Offset offset) {
      this.record = record;
      this.offset = offset;
    }
  }

  private class FutureWrapper {
    final Future<Record> future;
    final String sql;
    final int seq;

    public FutureWrapper(Future<Record> future, String sql, int seq) {
      this.future = future;
      this.sql = sql;
      this.seq = seq;
    }
  }

  private class RecordTxnInfo {
    final Record record;
    final String txnId;
    final LocalDateTime txnStartTime;

    private RecordTxnInfo(Record record, String txnId, LocalDateTime txnStartTime) {
      this.record = record;
      this.txnId = txnId;
      this.txnStartTime = txnStartTime;
    }
  }

  private class RecordErrorString {
    final Record record;
    final String errorString;

    private RecordErrorString(Record record, String errorString) {
      this.record = record;
      this.errorString = errorString;
    }
  }

  private class ErrorAndCause {
    final JdbcErrors error;
    final Throwable ex;

    private ErrorAndCause(JdbcErrors error, Throwable ex) {
      this.error = error;
      this.ex = ex;
    }
  }

}
