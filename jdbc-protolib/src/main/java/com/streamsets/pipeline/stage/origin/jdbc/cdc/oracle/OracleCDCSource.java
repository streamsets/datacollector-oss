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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle;

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
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
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
import com.streamsets.pipeline.lib.jdbc.parser.sql.UnparseableEmptySQLException;
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_00;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_40;
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
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_94;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.COMMIT_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.DDL_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.DELETE_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.INSERT_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.OPERATION;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.ROLLBACK_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.SELECT_FOR_UPDATE_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.UPDATE_CODE;
import static com.streamsets.pipeline.lib.jdbc.parser.sql.ParseUtil.JDBCTypeNames;
import static com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle.Groups.CDC;
import static com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle.Groups.CREDENTIALS;

public class OracleCDCSource extends BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(OracleCDCSource.class);
  private static final String CDB_ROOT = "CDB$ROOT";
  private static final String HIKARI_CONFIG_PREFIX = "hikariConf.";
  private static final String DRIVER_CLASSNAME = HIKARI_CONFIG_PREFIX + "driverClassName";
  private static final String USERNAME = HIKARI_CONFIG_PREFIX + "username";
  private static final String CONNECTION_STR = HIKARI_CONFIG_PREFIX + "connectionString";
  public static final String CHANGE_TYPES = "oracleCDCConfigBean.baseConfigBean.changeTypes";
  private static final String CURRENT_SCN = "SELECT CURRENT_SCN FROM GV$DATABASE";
  // At the time of executing this statement, either the cachedSCN is 0
  // (which means we are executing for the first time), or it is no longer valid, so select
  // only the ones that are > than the cachedSCN.
  private static final String GET_OLDEST_SCN =
      "SELECT FIRST_CHANGE#, STATUS from GV$ARCHIVED_LOG WHERE STATUS = 'A' AND FIRST_CHANGE# > ? ORDER BY FIRST_CHANGE#";
  private static final String SWITCH_TO_CDB_ROOT = "ALTER SESSION SET CONTAINER = CDB$ROOT";
  private static final String SET_SESSION_TIMEZONE = "ALTER SESSION SET TIME_ZONE = DBTIMEZONE";
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
  private static final String REDO_VALUE = PREFIX + "redoValue";
  private static final String UNDO_VALUE = PREFIX + "undoValue";
  private static final String PRECISION_TIMESTAMP = PREFIX + "precisionTimestamp";
  private static final String ORACLE_SEQUENCE = PREFIX + "sequence.oracle";
  private static final String INTERNAL_SEQUENCE = PREFIX + "sequence.internal";
  private static final String QUERY_KEY = PREFIX + "query";
  private static final String NULL = "NULL";
  private static final String OFFSET_VERSION_STR = "v2";
  private static final String OFFSET_VERSION_UNCOMMITTED = "v3";
  private static final String ZERO = "0";
  private static final String SCHEMA = "schema";
  private static final int MAX_RECORD_GENERATION_ATTEMPTS = 100;
  private static final int GET_TIME_FOR_SCN_MAX_ATTEMPS = 30;
  private static final int MINING_WAIT_TIME_MS = 1500;
  private static final int ORA_ERROR_TABLE_NOT_EXIST = 942;

  // What are all these constants?
  // String templates used in debug logging statements. To avoid unnecessarily creating new strings,
  // we just reuse these constants to avoid adding an if(LOG.isDebugEnabled()) call.
  private static final String XID = PREFIX + "xid";
  private static final String SEQ = "SEQ";
  private static final HashQueue<RecordSequence> EMPTY_LINKED_HASHSET = new InMemoryHashQueue<>(0);

  private static final String SENDING_TO_ERROR_AS_CONFIGURED = ". Sending to error as configured";
  private static final String UNSUPPORTED_TO_ERR = JDBC_85.getMessage() + SENDING_TO_ERROR_AS_CONFIGURED;
  private static final String DISCARDING_RECORD_AS_CONFIGURED = ". Discarding record as configured";
  private static final String UNSUPPORTED_DISCARD = JDBC_85.getMessage() + DISCARDING_RECORD_AS_CONFIGURED;
  private static final String UNSUPPORTED_SEND_TO_PIPELINE = JDBC_85.getMessage() + ". Sending to pipeline as configured";
  private static final String GENERATED_RECORD = "Generated Record: '{}' in transaction Id {}";
  public static final String FOUND_RECORDS_IN_TRANSACTION = "Found {} records in transaction ID {}";
  public static final String REDO_SELECT_QUERY = "Redo select query for selectFromLogMnrContents = {}";
  public static final String CURRENT_LATEST_SCN_IS = "Current latest SCN is: {}";
  private static final String SESSION_WINDOW_CURRENT_MSG = "Session window is in current time. Fetch size now set to {}";
  private static final String ROLLBACK_MESSAGE = "got rollback for {} - transaction discarded.";
  public static final int LOGMINER_START_MUST_BE_CALLED = 1306;
  public static final String ROLLBACK = "rollback";
  public static final String SKIP = "skip";
  public static final String ONE = "1";

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

  private BlockingQueue<RecordOffset> recordQueue;
  private final BlockingQueue<RecordErrorString> errorRecords = new LinkedBlockingQueue<>();
  private final BlockingQueue<String> unparseable = new LinkedBlockingQueue<>();
  private final BlockingQueue<RecordTxnInfo> expiredRecords = new LinkedBlockingQueue<>();
  private final BlockingQueue<ErrorAndCause> otherErrors = new LinkedBlockingQueue<>();

  private String selectString;
  private String offsetVersion;
  private ZoneId zoneId;
  private Record dummyRecord;
  private boolean useLocalBuffering;
  private boolean continuousMine;

  private Gauge<Map<String, Object>> delay;

  private static final String CONFIG_PROPERTY = "com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle.addrecordstoqueue";
  private static final boolean CONFIG_PROPERTY_DEFAULT_VALUE = false;
  private boolean useNewAddRecordsToQueue;

  private boolean checkBatchSize = true;

  private enum DDL_EVENT {
    CREATE,
    ALTER,
    DROP,
    TRUNCATE,
    STARTUP, // Represents event sent at startup.
    UNKNOWN
  }

  private static final String OFFSET_DELIM = "::";
  private static final int RESULTSET_CLOSED_AS_LOGMINER_SESSION_CLOSED = 1306;
  private static final String GET_CURRENT_TIMESTAMP = "SELECT CURRENT_TIMESTAMP FROM DUAL";
  private static final String NLS_DATE_FORMAT = "ALTER SESSION SET NLS_DATE_FORMAT = " + DateTimeColumnHandler.DT_SESSION_FORMAT;
  private static final String NLS_NUMERIC_FORMAT = "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = \'.,\'";
  private static final String NLS_TIMESTAMP_FORMAT =
      "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = " + DateTimeColumnHandler.TIMESTAMP_SESSION_FORMAT;
  private static final String NLS_TIMESTAMP_TZ_FORMAT =
      "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = " + DateTimeColumnHandler.ZONED_DATETIME_SESSION_FORMAT;
  private final Pattern ddlPattern = Pattern.compile("(CREATE|ALTER|DROP|TRUNCATE).*", Pattern.CASE_INSENSITIVE);

  private static final String TABLE_METADATA_TABLE_SCHEMA_CONSTANT = "TABLE_SCHEM";
  private static final String TABLE_METADATA_TABLE_NAME_CONSTANT = "TABLE_NAME";

  // Patterns to produce messages for JDBC_52 errors
  private static final String JDBC_52_LONG_PATTERN ="Action: %s - Message: %s - SQL State: %s - Vendor Code: %s";
  private static final String JDBC_52_SHORT_PATTERN ="Action: %s - Message: %s";

  private final OracleCDCConfigBean configBean;
  private final HikariPoolConfigBean hikariConfigBean;
  private final List<TableFilter> tableFilters = new ArrayList<>();
  private final Map<SchemaAndTable, Map<String, Integer>> tableSchemas = new HashMap<>();
  private final Map<SchemaAndTable, Map<String, String>> dateTimeColumns = new HashMap<>();
  private final Map<SchemaAndTable, Map<String, PrecisionAndScale>> decimalColumns = new HashMap<>();
  private final Map<SchemaAndTable, BigDecimal> tableSchemaLastUpdate = new HashMap<>();

  private final ExecutorService generationExecutor =
      Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Oracle CDC Data Generator").build());

  private volatile boolean generationStarted = false;
  private final boolean shouldTrackDDL;

  private int databaseVersion;
  private LogMinerSession logMinerSession;
  private ErrorRecordHandler errorRecordHandler;
  private boolean containerized = false;

  private HikariDataSource dataSource = null;
  private Connection connection = null;
  private PreparedStatement getLatestSCN;
  private PreparedStatement dateStatement;
  private PreparedStatement tsStatement;
  private PreparedStatement numericFormat;
  private PreparedStatement switchContainer;
  private PreparedStatement tsTzStatement;
  private PreparedStatement setSessionTimezone;

  private final ThreadLocal<ParseTreeWalker> parseTreeWalker = ThreadLocal.withInitial(ParseTreeWalker::new);
  private final ThreadLocal<SQLListener> sqlListener = ThreadLocal.withInitial(SQLListener::new);
  private final ThreadLocal<SQLParser> sqlParser =
      ThreadLocal.withInitial(() -> Parboiled.createParser(SQLParser.class));

  private ExecutorService parsingExecutor;

  private final JdbcUtil jdbcUtil;

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
    if (!getContext().isPreview() && checkBatchSize && configBean.baseConfigBean.maxBatchSize > maxBatchSize) {
      getContext().reportError(JDBC_502, maxBatchSize);
      checkBatchSize = false;
    }

    int recordGenerationAttempts = 0;
    boolean recordsProduced = false;
    String nextOffset = StringUtils.trimToEmpty(lastSourceOffset);
    pollForStageExceptions();
    while (!getContext().isStopped() && !recordsProduced && recordGenerationAttempts++ < MAX_RECORD_GENERATION_ATTEMPTS) {
      if (!sentInitialSchemaEvent) {
        for (SchemaAndTable schemaAndTable : tableSchemas.keySet()) {
          getContext().toEvent(
              createEventRecord(DDL_EVENT.STARTUP, null, schemaAndTable, ZERO, true, null));
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

  private void startGeneratorThread(String lastSourceOffset) throws StageException {
    Offset offset = null;
    LocalDateTime startTimestamp;
    LocalDateTime endTimestamp;
    boolean logMinerStarted = false;
    try {
      if (!StringUtils.isEmpty(lastSourceOffset)) {
        offset = new Offset(lastSourceOffset);
        if (offset.version.equals(OFFSET_VERSION_UNCOMMITTED)) {
          if (!useLocalBuffering) {
            throw new StageException(JDBC_82);
          }
          startTimestamp = offset.timestamp.minusSeconds(configBean.txnWindow);
        } else {
          if (useLocalBuffering) {
            throw new StageException(JDBC_83);
          }
          startTimestamp = logMinerSession.getLocalDateTimeForSCN(new BigDecimal(offset.scn), GET_TIME_FOR_SCN_MAX_ATTEMPS);
        }
        offset.timestamp = startTimestamp;
        endTimestamp = getEndTimeForStartTime(startTimestamp);
        logMinerStarted = startLogMiner(startTimestamp, endTimestamp);
      } else { // reset the start date only if it not set.
        if (configBean.startValue != StartValues.SCN) {
          LocalDateTime startDate;
          if (configBean.startValue == StartValues.DATE) {
            startDate = LocalDateTime.parse(configBean.startDate, dateTimeColumnHandler.dateFormatter);
          } else {
            startDate = nowAtDBTz();
          }
          endTimestamp = getEndTimeForStartTime(startDate);
          logMinerStarted = startLogMiner(startDate, endTimestamp);
          offset = new Offset(offsetVersion, startDate, ZERO, 0,"");
        } else {
          BigDecimal startCommitSCN = new BigDecimal(configBean.startSCN);
          final LocalDateTime start = logMinerSession.getLocalDateTimeForSCN(startCommitSCN, GET_TIME_FOR_SCN_MAX_ATTEMPS);
          endTimestamp = getEndTimeForStartTime(start);
          logMinerStarted = startLogMiner(start, endTimestamp);
          offset = new Offset(offsetVersion, start, startCommitSCN.toPlainString(), 0, "");
        }
      }
    } catch (StageException e) {
      LOG.error("Error while trying to setup record generator thread", e);
      generationStarted = false;
      throw new StageException(JdbcErrors.JDBC_52,
          String.format(JDBC_52_SHORT_PATTERN, "Start generator thread", e.getMessage())
      );
    }
    final Offset os = offset;
    final boolean started = logMinerStarted;
    generationExecutor.submit(() -> {
      try {
        generateRecords(os, endTimestamp, started);
      } catch (Throwable ex) {
        LOG.error("Error while producing records", ex);
        generationStarted = false;
      }
    });
  }

  private boolean startLogMiner(LocalDateTime startDate, LocalDateTime endDate) throws StageException {
    boolean preloadDictionary = (configBean.dictionary == DictionaryValues.DICT_FROM_REDO_LOGS);
    return startLogMiner(startDate, endDate, preloadDictionary);
  }

  private boolean startLogMiner(LocalDateTime start, LocalDateTime end, boolean preloadDictionary) {
    if (preloadDictionary) {
      if (!logMinerSession.preloadDictionary(start)) {
        return false;
      }
    }
    return logMinerSession.start(start, end);
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
      recreateLogMinerSession();
      initializeStatements();
      alterSession();
    }
  }

  /**
   * Re-create the LogMinerSession utilized by the stage according to its configuration.
   */
  private void recreateLogMinerSession() throws SQLException {
    logMinerSession = new LogMinerSession.Builder(connection, databaseVersion)
        .setContinuousMine(continuousMine)
        .setDictionarySource(configBean.dictionary)
        .setDDLDictTracking(shouldTrackDDL)
        .setCommittedDataOnly(!useLocalBuffering)
        .setTablesForMining(configBean.baseConfigBean.schemaTableConfigs)
        .setTrackedOperations(configBean.baseConfigBean.changeTypes)
        .build();
  }

  private long getDelay(LocalDateTime lastEndTime) {
    // Avoid to query Oracle to get the database time -- getDelay can be invoked thousands of times per second.
    LocalDateTime dbTime = LocalDateTime.now(zoneId);
    return localDateTimeToEpoch(dbTime) - localDateTimeToEpoch(lastEndTime);
  }

  private void generateRecords(Offset startingOffset, LocalDateTime endTime, boolean logMinerStarted) {
    // When this is called the first time, Logminer was started either from SCN or from a start date, so we just keep
    // track of the start date etc.
    LOG.info("Attempting to generate records");
    boolean error;
    StringBuilder sqlRedoBuilder = new StringBuilder();
    LogMinerResultSetWrapper resultSet = null;
    BigDecimal lastCommitSCN = new BigDecimal(startingOffset.scn);
    int sequenceNumber = startingOffset.sequence;
    LocalDateTime startTime = adjustStartTime(startingOffset.timestamp);
    String lastTxnId = startingOffset.txnId;
    endTime = logMinerStarted ? endTime : getEndTimeForStartTime(startTime);
    boolean sessionWindowInCurrent = inSessionWindowCurrent(startTime, endTime);

    while (!getContext().isStopped()) {
      error = false;
      generationStarted = true;
      if (logMinerStarted) {
        try {
          Offset offset = new Offset(offsetVersion, startTime, lastCommitSCN.toPlainString(), sequenceNumber, lastTxnId);
          recordQueue.put(new RecordOffset(dummyRecord, offset));
          resultSet = useLocalBuffering ? logMinerSession.queryContent(startTime, endTime)
                                        : logMinerSession.queryContent(startTime, endTime, lastCommitSCN, sequenceNumber);

          if (!sessionWindowInCurrent) {
            resultSet.setFetchSize(configBean.jdbcFetchSize);
          } else {
            LOG.trace(SESSION_WINDOW_CURRENT_MSG, configBean.fetchSizeLatest);
            resultSet.setFetchSize(configBean.fetchSizeLatest);
          }

          while (resultSet.next() && !getContext().isStopped()) {
            LogMinerRecord logMnrRecord = resultSet.getRecord();
            sqlRedoBuilder.append(logMnrRecord.getSqlRedo());

            // A CDC record is split into several rows when the SQL statement is longer than 4000 bytes. When that
            // happens, complete SQL statement before continuing the process.
            if (!logMnrRecord.isEndOfRecord()) {
              continue;
            }
            String sqlRedo = sqlRedoBuilder.toString();
            sqlRedoBuilder.setLength(0);

            TransactionIdKey key = new TransactionIdKey(logMnrRecord.getXID());
            delay.getValue().put("delay", getDelay(logMnrRecord.getLocalDateTime()));

            int bufferedRecordsSize = 0;
            int totRecs = 0;
            bufferedRecordsLock.lock();
            try {
              if (useLocalBuffering &&
                  bufferedRecords.containsKey(key) &&
                  bufferedRecords.get(key).contains(new RecordSequence(null, null, 0, 0,
                      logMnrRecord.getRsId(), logMnrRecord.getSsn(), logMnrRecord.getRowId(), null))) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Discarding LogMiner entry as it is already buffered: {}", logMnrRecord.toString());
                }
                continue;
              }
              if (LOG.isDebugEnabled()) {
                bufferedRecordsSize = bufferedRecords.size();
                for (Map.Entry<OracleCDCSource.TransactionIdKey, HashQueue<RecordSequence>> r : bufferedRecords.entrySet()) {
                  totRecs += r.getValue().size();
                }
              }
            } finally {
              bufferedRecordsLock.unlock();
            }

            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Num Active Txns = {} Total Cached Records = {} Commit SCN = {}, SCN = {}, Operation = {}, Txn Id =" +
                      " {}, Timestamp = {}, Row Id = {}, Redo SQL = {}",
                  bufferedRecordsSize,
                  totRecs,
                  logMnrRecord.getCommitSCN() == null ? "" : logMnrRecord.getCommitSCN().toPlainString(),
                  logMnrRecord.getScn().toPlainString(),
                  logMnrRecord.getOperationCode(),
                  logMnrRecord.getXID(),
                  logMnrRecord.getLocalDateTime(),
                  logMnrRecord.getRowId(),
                  sqlRedo
              );
            }

            switch (logMnrRecord.getOperationCode()) {
              case INSERT_CODE:
              case DELETE_CODE:
              case UPDATE_CODE:
              case SELECT_FOR_UPDATE_CODE:
                boolean accepted;
                if (useLocalBuffering) {
                  accepted = updateBufferedTransaction(key, logMnrRecord, sqlRedo);
                } else {
                  accepted = enqueueRecord(logMnrRecord, sqlRedo, lastCommitSCN, sequenceNumber);
                  if (accepted) {
                    lastCommitSCN = logMnrRecord.getCommitSCN();
                    sequenceNumber = logMnrRecord.getSequence();
                  }
                }
                if (!accepted) {
                  LOG.debug("Discarding record: SCN = {}, Redo SQL = '{}'",
                      logMnrRecord.getScn().toPlainString(),
                      sqlRedo
                  );
                }
                break;

              case COMMIT_CODE:
              case ROLLBACK_CODE:
                if (useLocalBuffering) {
                  int seq = processBufferedTransaction(key, logMnrRecord, lastCommitSCN, sequenceNumber, lastTxnId);
                  if (seq >= 0) {
                    lastCommitSCN = logMnrRecord.getScn();
                    sequenceNumber = seq;
                    lastTxnId = logMnrRecord.getXID();
                  }
                }
                break;

              case DDL_CODE:
                if (!getContext().isPreview()) {
                  if (!generateEvent(logMnrRecord, sqlRedo)) {
                    LOG.debug("Discarding record: SCN = {}, Redo SQL = '{}'",
                        logMnrRecord.getScn().toPlainString(),
                        sqlRedo
                    );
                  }
                }
                break;
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
            addToStageExceptionsQueue(new StageException(JDBC_52, String.format(
                JDBC_52_LONG_PATTERN,
                "Get local date & time for SCN",
                ex.getMessage(),
                ex.getSQLState(),
                ex.getErrorCode()
            )));
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
          addToStageExceptionsQueue(new StageException(
              JDBC_52,
              String.format(JDBC_52_SHORT_PATTERN, "Generate records (Exception)", ex.getMessage())
          ));
        } finally {
          // If an incomplete batch is seen, it means we are going to move the window forward
          // Ending this session and starting a new one helps reduce PGA memory usage.
          try {
            if (resultSet != null && !resultSet.isClosed()) {
              resultSet.close();
            }
          } catch (SQLException ex) {
            LOG.warn("Error while attempting to close ResultSet", ex);
          }
          try {
            if (error) {
              resetConnectionsQuietly();
            } else {
              discardOldUncommitted(startTime);

              if (continuousMine) {
                // When CONTINUOUS_MINE is enabled, explicitly close LogMiner session to ensure PGA resources are
                // released. This is not needed when CONTINUOUS_MINE is disabled because LogMinerSession#start reuse
                // the current session and redo logs are manually loaded/removed according to the specified time range.
                try {
                  logMinerSession.close();
                } catch (SQLException ex) {
                  LOG.error("Error while attempting to close the current LogMinerSession", ex);
                }
              }

              if (logMinerSession.isSessionIntegrityGuaranteed()) {
                if (sessionWindowInCurrent) {
                  sleepCurrentThread(MINING_WAIT_TIME_MS);
                }
                startTime = adjustStartTime(endTime);
                endTime = getEndTimeForStartTime(startTime);
              } else {
                LOG.info("LogMiner session integrity cannot be guaranteed. There is at least one online redo log that " +
                    "could have been rotated while mining it. Retrying process for the current mining window...");
              }
            }
          } catch (StageException ex) {
            LOG.error("Error while attempting to prepare a new LogMinerSession", ex);
            addToStageExceptionsQueue(ex);
          }
        }
      }
      try {
        boolean preloadDictionary = (configBean.dictionary == DictionaryValues.DICT_FROM_REDO_LOGS) && (continuousMine || error);
        logMinerStarted = startLogMiner(startTime, endTime, preloadDictionary);
        endTime = logMinerSession.getEndTime();  // This might have been moved forward by LogMinerSession#start. Update
                                                 // to avoid the next window overlap with the current one.
        sessionWindowInCurrent = inSessionWindowCurrent(startTime, endTime);
        if (!logMinerStarted) {
          // This can happen when 'endTime' is momentarily ahead of the current redo log, which in turn can happen when
          // a log rotation is in progress. In that case, sleep for a while and try again.
          LOG.info("Could not start LogMiner session ({}, {}). Sleep thread and retry...", startTime, endTime);
          sleepCurrentThread(MINING_WAIT_TIME_MS);
        }

      } catch (StageException ex) {
        LOG.error("Error while attempting to start LogMiner", ex);
        addToStageExceptionsQueue(ex);
      }
    }
  }

  private void sleepCurrentThread(int milliseconds) {
    try {
      Thread.sleep(milliseconds);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Creates and enqueues a new SDC record.
   *
   * Returns true if the record was enqueued, either into {@link OracleCDCSource#recordQueue} (when sqlRedo was
   * successfully parsed) or {@link OracleCDCSource#unparseable} (when parsing error). Returns false when the record
   * was ignored because its COMMIT_SCN is behind {@code lastCommitSCN} or affects a table not tracked.
   */
  private boolean enqueueRecord(
      LogMinerRecord logMnrRecord,
      String sqlRedo,
      BigDecimal lastCommitSCN,
      int sequenceNumber
  ) throws InterruptedException {
    SchemaAndTable schemaAndTable = new SchemaAndTable(logMnrRecord.getSegOwner(), logMnrRecord.getTableName());
    if (!tableSchemas.containsKey(schemaAndTable)) {
      // If the table schema is not cached it means that the table was dropped prior to reaching this point.
      // In that case, discard this record.
      return false;
    }
    // Discard any record behind the most recent enqueued one.
    if (logMnrRecord.getCommitSCN().compareTo(lastCommitSCN) < 0 ||
        (logMnrRecord.getCommitSCN().compareTo(lastCommitSCN) == 0 && logMnrRecord.getSequence() < sequenceNumber)) {
      return false;
    }

    Map<String, String> attributes = createRecordHeaderAttributes(logMnrRecord, sqlRedo);
    Offset offset = new Offset(offsetVersion, logMnrRecord.getLocalDateTime(), logMnrRecord.getCommitSCN().toPlainString(),
          logMnrRecord.getSequence(), logMnrRecord.getXID());

    try {
      Record record = generateRecord(sqlRedo, attributes, logMnrRecord.getOperationCode());
      if (record != null && record.getEscapedFieldPaths().size() > 0) {
        recordQueue.put(new RecordOffset(record, offset));
      }
    } catch (UnparseableSQLException ex) {
      LOG.error("Parsing failed", ex);
      unparseable.offer(sqlRedo);
    }

    return true;
  }

  /**
   * Update a buffered transaction (or creates a new one if not previously buffered) with a new database operation
   * defined by {@code logMnrRecord} and {@code sqlRedo}.
   *
   * Returns false when the operation is ignored because it affects a table not tracked.
   */
  private boolean updateBufferedTransaction(TransactionIdKey key, LogMinerRecord logMnrRecord, String sqlRedo) {
    SchemaAndTable schemaAndTable = new SchemaAndTable(logMnrRecord.getSegOwner(), logMnrRecord.getTableName());
    if (!tableSchemas.containsKey(schemaAndTable)) {
      // If the table schema is not cached it means that the table was dropped prior to reaching this point.
      // In that case, discard this record.
      return false;
    }

    Map<String, String> attributes = createRecordHeaderAttributes(logMnrRecord, sqlRedo);
    bufferedRecordsLock.lock();
    try {
      HashQueue<RecordSequence> records =
          bufferedRecords.computeIfAbsent(key, x -> {
            x.setTxnStartTime(logMnrRecord.getLocalDateTime());
            return createTransactionBuffer(key.txnId);
          });

      int nextSeq = records.isEmpty() ? 1 : records.tail().seq + 1;
      RecordSequence node = new RecordSequence(attributes, sqlRedo, nextSeq, logMnrRecord.getOperationCode(),
          logMnrRecord.getRsId(), logMnrRecord.getSsn(), logMnrRecord.getRowId(), logMnrRecord.getLocalDateTime());

      // Check for SAVEPOINT/ROLLBACK.
      // When we get a record with the ROLLBACK indicator set, we go through all records in the transaction,
      // find the last one with the matching rowId. Save its position if and only if it has not previously been
      // marked as a record to SKIP.
      // ONLY use this code when we are using the new version of addRecordsToQueue() the old version of
      // addRecordsToQueue() includes the rollback code and does not support "SKIP"ping Records.
      if(node.headers.get(ROLLBACK).equals(ONE) && useNewAddRecordsToQueue) {
        int lastOne = -1;
        int count = 0;
        for(RecordSequence rs : records) {
          if(rs.headers.get(ROWID_KEY).equals(node.headers.get(ROWID_KEY))
              && rs.headers.get(SKIP) == null) {
            lastOne = count;
          }
          count++;
        }
        // Go through the records again, and update the lastOne that has the specific rowId to indicate we should not
        // process it. The original "ROLLBACK" record will *not* be added to the transaction.
        count = 0;
        for(RecordSequence rs : records) {
          if(count == lastOne) {
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
    return true;
  }

  /**
   * Processes a buffered transaction accordingly to {@code record}, which must contain a ROLLBACK or a COMMIT
   * operation.
   *
   * When the operation is COMMIT and its SCN is ahead {@code lastCommitSCN}, the buffered SDC records belonging to
   * this transaction are enqueued and a SEQUENCE# is returned. Otherwise the function returns -1.
   */
  private int processBufferedTransaction(TransactionIdKey key, LogMinerRecord record, BigDecimal lastCommitSCN,
      int sequenceNumber, String lastTxnId) throws InterruptedException {
    int result = -1;

    // Discard transaction if this commit was previously processed or it is a rollback.
    if (record.getOperationCode() == ROLLBACK_CODE || record.getScn().compareTo(lastCommitSCN) < 0) {
      bufferedRecordsLock.lock();
      try {
        HashQueue<RecordSequence> records = bufferedRecords.getOrDefault(key, EMPTY_LINKED_HASHSET);
        records.completeInserts();
        records.close();
        bufferedRecords.remove(key);
        LOG.debug(ROLLBACK_MESSAGE, key.txnId);
      } finally {
        bufferedRecordsLock.unlock();
      }
    } else {
      bufferedRecordsLock.lock();
      try {
        HashQueue<RecordSequence> transactionRecords = bufferedRecords.getOrDefault(key, EMPTY_LINKED_HASHSET);
        if (lastCommitSCN.equals(record.getScn()) && record.getXID().equals(lastTxnId)) {
          removeProcessedRecords(transactionRecords, sequenceNumber);
        }
        int bufferedRecordsToBeRemoved = transactionRecords.size();
        LOG.debug(FOUND_RECORDS_IN_TRANSACTION, bufferedRecordsToBeRemoved, record.getXID());

        if(useNewAddRecordsToQueue) {
          result = addRecordsToQueue(record.getLocalDateTime(), record.getScn().toPlainString(), record.getXID());
        } else {
          result = addRecordsToQueueOLD(record.getLocalDateTime(), record.getScn().toPlainString(), record.getXID());
        }

      } finally {
        bufferedRecordsLock.unlock();
      }
    }

    return result;
  }

  private Map<String, String> createRecordHeaderAttributes(LogMinerRecord record, String sqlRedo) {
    Map<String, String> attributes = new HashMap<>();
    attributes.put(SCN, record.getScn().toPlainString());
    attributes.put(USER, record.getUserName());
    attributes.put(TIMESTAMP_HEADER, record.getTimestamp());
    attributes.put(TABLE, record.getTableName());
    attributes.put(SEQ, String.valueOf(record.getSequence()));
    attributes.put(XID, record.getXID());
    attributes.put(RS_ID, record.getRsId());
    attributes.put(SSN, record.getSsn().toString());
    attributes.put(SCHEMA, record.getSegOwner());
    attributes.put(ROLLBACK, String.valueOf(record.getRollback()));
    attributes.put(ROWID_KEY, record.getRowId());
    attributes.put(record.getRedoValue() == null ? "-1" : REDO_VALUE, record.getRedoValue().toPlainString());
    attributes.put(record.getUndoValue() == null ? "-1" : UNDO_VALUE, record.getUndoValue().toPlainString());
    attributes.put(PRECISION_TIMESTAMP, record.getPrecisionTimestamp().toString());
    attributes.put(ORACLE_SEQUENCE, "" + record.getSequence());

    if (configBean.keepOriginalQuery) {
      attributes.put(QUERY_KEY, sqlRedo);
    }

    return attributes;
  }

  /**
   * Creates and enqueue a new event record for a DLL operation, defined by {@code record} and {@code sqlRedo}.
   *
   * Returns false if the record was ignored because of affecting a table not tracked.
   */
  private boolean generateEvent(LogMinerRecord record, String sqlRedo) throws InterruptedException, SQLException {
    // Event is sent on every DDL, but schema is not always sent.
    // Schema sending logic:
    // CREATE/ALTER: Schema is sent if the schema after the ALTER is newer than the cached schema
    // (which we would have sent as an event earlier, at the last alter)
    // DROP/TRUNCATE: Schema is not sent, since they don't change schema.
    boolean sendSchema = false;
    boolean removedSchema = false;
    DDL_EVENT type = getDdlType(sqlRedo);
    SchemaAndTable schemaAndTable = new SchemaAndTable(record.getSegOwner(), record.getTableName());

    if (type == DDL_EVENT.ALTER || type == DDL_EVENT.CREATE) {
      if (validateWithNameFilters(schemaAndTable)) {
        sendSchema = refreshCachedSchema(record.getScn(), schemaAndTable);
      }
    } else if (type == DDL_EVENT.DROP) {
      removedSchema = removeCachedSchema(schemaAndTable);
    }

    // Only send DDL events for the tracked tables in the database. Tracked tables are those that existed
    // during the pipeline initialization or were created after that.
    boolean accepted = tableSchemas.containsKey(schemaAndTable) || removedSchema;
    if (accepted) {
      Offset offset = new Offset(offsetVersion, record.getLocalDateTime(),
          record.getScn().toPlainString(), 0, record.getXID());

      EventRecord event = createEventRecord(
          type,
          sqlRedo,
          schemaAndTable,
          offset.toString(),
          sendSchema,
          record.getTimestamp()
      );

      recordQueue.put(new RecordOffset(event, offset));
    }
    return accepted;
  }

  /**
   * Returns True if the mining window defined by {@code startTime} and {@code endTime} reaches the current database
   * time.
   *
   * NOTE: see also {@link OracleCDCSource#getEndTimeForStartTime} and {@link LogMinerResultSetWrapper#next}.
   */
  private boolean inSessionWindowCurrent(LocalDateTime startTime, LocalDateTime endTime) {
    boolean reached;
    if (continuousMine) {
      LocalDateTime currentTime = nowAtDBTz();
      reached = (currentTime.isAfter(startTime) && currentTime.isBefore(endTime))
          || currentTime.isEqual(startTime) || currentTime.isEqual(endTime);
    } else {
      reached = Duration.between(startTime, endTime).toMillis() < configBean.logminerWindow * 1000;
    }
    return reached;
  }

  private void resetConnectionsQuietly() {
    try {
      resetDBConnectionsIfRequired();
    } catch (SQLException sqlEx) {
      LOG.error("Error while connecting to DB", sqlEx);
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

  /**
   * Parses a SQL statement and generates an SDC record.
   *
   * @param sql SQL statement to parse.
   * @param attributes Header attributes to include in the generated SDC record.
   * @param operationCode A valid {@code OracleCDCOperationCode} value.
   * @return An SDC record or null if the SQL statement contains an unsupported database type and
   *     {@code configBean.unsupportedFieldOp} is not {@code SEND_TO_PIPELINE}.
   */
  private Record generateRecord(
      String sql,
      Map<String, String> attributes,
      int operationCode
  ) throws UnparseableSQLException, StageException {
    String operation;
    SchemaAndTable table = new SchemaAndTable(attributes.get(SCHEMA), attributes.get(TABLE));
    int sdcOperationType = ParseUtil.getOperation(operationCode);
    operation = OperationType.getLabelFromIntCode(sdcOperationType);
    attributes.put(OperationType.SDC_OPERATION_TYPE, String.valueOf(sdcOperationType));
    attributes.put(OPERATION, operation);
    String id = useLocalBuffering ?
        attributes.get(RS_ID) + OFFSET_DELIM + attributes.get(SSN) :
        attributes.get(SCN) + OFFSET_DELIM + attributes.get(SEQ);
    Record record = getContext().createRecord(id);
    boolean emptySQL = false;
    if (configBean.parseQuery) {
      Map<String, String> columns = new HashMap<>();
      if (configBean.useNewParser) {
        Set<String> columnsExpected = null;
        if (configBean.allowNulls && table.isNotEmpty()) {
          columnsExpected = tableSchemas.get(table).keySet();
        }
        try {
          columns = SQLParserUtils.process(
            sqlParser.get(),
            sql,
            operationCode,
            configBean.allowNulls,
            configBean.baseConfigBean.caseSensitive,
            columnsExpected
          );
        } catch (UnparseableEmptySQLException e) {
          LOG.debug("Empty Redo Log SQL: '{}'. That is probably caused by a column type not supported by LogMiner.", e.getMessage());
          emptySQL = true;
        }
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
        if (StringUtils.isBlank(sql)) {
          LOG.debug("Empty Redo Log SQL: That is probably caused by a column type not supported by LogMiner.");
          emptySQL = true;
        } else {
          parseTreeWalker.get().walk(sqlListener.get(), ParseUtil.getParserRuleContext(sql, operationCode));
          columns = sqlListener.get().getColumns();
        }
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
              String fieldTypeName =
                  JDBCTypeNames.getOrDefault(ex.fieldType, "unknown");
              return "[Column = '" + ex.column + "', Type = '" + fieldTypeName + "', Value = '" + ex.columnVal + "']";
            }
        ).collect(Collectors.toList());
      }

      if (!fieldTypeExceptions.isEmpty() || emptySQL) {
        String errorString = fieldTypeExceptions.isEmpty() ? "LogMiner returned empty SQL Redo statement." : errorStringJoiner.join(errorColumns);
        boolean add = handleUnsupportedFieldTypes(record, errorString, emptySQL);
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

  private boolean handleUnsupportedFieldTypes(Record r, String error, boolean emptySQL) {
    switch (configBean.unsupportedFieldOp) {
      case SEND_TO_PIPELINE:
        if (LOG.isDebugEnabled()) {
          LOG.debug(UNSUPPORTED_SEND_TO_PIPELINE, error, r.getHeader().getAttribute(TABLE));
        }
        return !emptySQL;
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
      LocalDateTime commitTimestamp,
      String commitScn,
      String xid
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
    int sequence = 0;
    while (!records.isEmpty()) {
      RecordSequence r = records.remove();
      if (configBean.keepOriginalQuery) {
        r.headers.put(QUERY_KEY, r.sqlString);
      }
      r.headers.put(INTERNAL_SEQUENCE, "" + sequence);
      sequence++;
      final Future<Record> recordFuture = parsingExecutor.submit(() -> generateRecord(r.sqlString, r.headers, r.opCode));
      parseFutures.add(new FutureWrapper(recordFuture, r.sqlString, r.seq));
    }
    records.close();
    LinkedList<RecordOffset> recordOffsets = new LinkedList<>();
    for (FutureWrapper recordFuture : parseFutures) {
      try {
        Record record = recordFuture.future.get();
        if (record != null) {
          final RecordOffset recordOffset =
              new RecordOffset(record, new Offset(OFFSET_VERSION_UNCOMMITTED, commitTimestamp, commitScn, recordFuture.seq, xid));

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

  private int addRecordsToQueue(LocalDateTime commitTimestamp, String commitScn, String xid) {
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
    int sequence = 0;
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
              r.sqlString, r.headers.get(ROWID_KEY)
          );
          continue;
        }

        r.headers.put(INTERNAL_SEQUENCE, "" + sequence);
        sequence++;

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

          if (record != null) {
            recordOffset = new RecordOffset(
                record, new Offset(OFFSET_VERSION_UNCOMMITTED, commitTimestamp, commitScn, recordFuture.seq, xid));
            seq = recordOffset.offset.sequence;

            while (!recordQueue.offer(recordOffset, 1, TimeUnit.SECONDS)) {
              if (getContext().isStopped()) {
                records.close();
                return seq;
              }
            }
            LOG.trace(GENERATED_RECORD, recordOffset.record, recordOffset.record.getHeader().getAttribute(XID));
          }
        } catch (InterruptedException | ExecutionException ex) {
          String errorSql = recordFuture != null ? (recordFuture.sql != null ? recordFuture.sql : "Empty"): "Unknown";
          LOG.error(JDBC_405.getMessage(), errorSql, ex);
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
      DDL_EVENT type,
      String redoSQL,
      SchemaAndTable schemaAndTable,
      String scnSeq,
      boolean sendSchema,
      String timestamp
  ) {
    EventRecord event = getContext().createEventRecord(type.name(), 1, scnSeq);
    event.getHeader().setAttribute(TABLE, schemaAndTable.getTable());
    if (redoSQL != null) {
      event.getHeader().setAttribute(DDL_TEXT, redoSQL);
    }
    if (timestamp != null) {
      event.getHeader().setAttribute(TIMESTAMP_HEADER, timestamp);
    }

    Map<String, Field> fields = new HashMap<>();
    if (sendSchema) {
      // Note that the schema inserted is the *current* schema and not the result of the DDL.
      // Getting the schema as a result of the DDL is not possible.
      // We actually don't know the schema at table creation ever, but just the schema when we started. So
      // trying to figure out the schema at the time of the DDL is not really possible since this DDL could have occured
      // before the source started. Since we allow only types to be bigger and no column drops, this is ok.
      Map<String, Integer> schema = tableSchemas.get(schemaAndTable);
      for (Map.Entry<String, Integer> column : schema.entrySet()) {
        fields.put(column.getKey(), Field.create(JDBCTypeNames.get(column.getValue())));
      }
    }
    event.set(Field.create(fields));
    LOG.debug("Event produced: " + event);

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
   * Validate the given schema and table names match one of the name filters configured in the stage.
   *
   * @param schemaAndTable The schema and table name to validate.
   * @return True if one of the schema and table patterns
   */
  private boolean validateWithNameFilters(SchemaAndTable schemaAndTable) {
    return tableFilters.stream().anyMatch(filter ->
      filter.schemaPattern.matcher(schemaAndTable.getSchema()).matches()
          && filter.tablePattern.matcher(schemaAndTable.getTable()).matches()
          && (filter.exclusionPattern == null || !filter.exclusionPattern.matcher(schemaAndTable.getTable()).matches())
    );
  }

  /**
   * Refresh the schema for the table if the last update of this table was before the given SCN.
   * Returns true if it was updated, else returns false. When returning false, it is caused by either:
   * - The given SCN is older than the cached schema for this table.
   * - The table currently does not exist in the database.
   *
   * To disambiguate between them, you can check the table schema cache ({@link OracleCDCSource#tableSchemas} map). If
   * it does not have an entry for the given table, then the table does not exist.
   */
  private boolean refreshCachedSchema(BigDecimal scnDecimal, SchemaAndTable schemaAndTable) throws SQLException {
    try {
      if (!tableSchemaLastUpdate.containsKey(schemaAndTable)
          || scnDecimal.compareTo(tableSchemaLastUpdate.get(schemaAndTable)) > 0) {
        if (containerized) {
          try (Statement switchToPdb = connection.createStatement()) {
            switchToPdb.execute("ALTER SESSION SET CONTAINER = " + configBean.pdb);
          }
        }
        tableSchemas.put(schemaAndTable, getTableSchema(schemaAndTable));
        tableSchemaLastUpdate.put(schemaAndTable, scnDecimal);
        return true;
      }
    } catch (SQLException e) {
      if (e.getErrorCode() != ORA_ERROR_TABLE_NOT_EXIST) {
        throw e;
      }
    } finally {
      alterSession();
    }
    return false;
  }

  /**
   * Delete the cached schema for a given table.
   *
   * @param schemaAndTable The table to be removed from the cache.
   * @return True if the table was removed, false if the table did not existed in the cache.
   */
  private boolean removeCachedSchema(SchemaAndTable schemaAndTable) {
    tableSchemaLastUpdate.remove(schemaAndTable);
    return tableSchemas.remove(schemaAndTable) != null;
  }

  private void addToStageExceptionsQueue(StageException ex) {
    try {
      while(!stageExceptions.offer(ex, 1, TimeUnit.SECONDS) && !getContext().isStopped());
    } catch (InterruptedException iEx) {
      LOG.error("Interrupted while adding stage exception to queue");
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Compute the upper bound for the next mining window given its lower bound.
   *
   * When CONTINUOUS_MINE option is enabled, we allow the mining window to span across the future. When not enabled,
   * we limit the upper bound to a value no further than the current database time. See
   * {@link LogMinerResultSetWrapper#next()} for more details about this.
   *
   * @param startTime The lower bound for the next mining window.
   * @return The mining window's upper bound.
   */
  private LocalDateTime getEndTimeForStartTime(LocalDateTime startTime) {
    LocalDateTime endTime = startTime.plusSeconds(configBean.logminerWindow);

    if (!continuousMine) {
      // Ensure the LogMiner window does not span further than the current time in database.
      LocalDateTime dbTime = nowAtDBTz();
      endTime = endTime.isAfter(dbTime) ? dbTime : endTime;
    }
    return endTime;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    // Upcase schema and table names unless caseSensitive config is enabled.
    for (SchemaTableConfigBean tables : configBean.baseConfigBean.schemaTableConfigs) {
      tables.schema = configBean.baseConfigBean.caseSensitive ? tables.schema : tables.schema.toUpperCase();
      tables.table = configBean.baseConfigBean.caseSensitive ? tables.table : tables.table.toUpperCase();
      if (tables.excludePattern != null) {
        tables.excludePattern = configBean.baseConfigBean.caseSensitive ?
                                tables.excludePattern : tables.excludePattern.toUpperCase();
      }
    }

    // save configuration parameter
    useNewAddRecordsToQueue = getContext().getConfiguration().get(CONFIG_PROPERTY, CONFIG_PROPERTY_DEFAULT_VALUE);
    LOG.info("init():  {} = {} ", CONFIG_PROPERTY, useNewAddRecordsToQueue);

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    useLocalBuffering = !getContext().isPreview() && configBean.bufferLocally;

    if (!hikariConfigBean.driverClassName.isEmpty()) {
      try {
        Class.forName(hikariConfigBean.driverClassName);
      } catch (ClassNotFoundException e) {
        LOG.error("Hikari Driver class not found.", e);
        issues.add(getContext().createConfigIssue(
            Groups.LEGACY.name(), DRIVER_CLASSNAME, JdbcErrors.JDBC_28, e.toString()));
      }
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

    if (configBean.parseQuery && configBean.baseConfigBean.changeTypes.contains(ChangeTypeValues.SELECT_FOR_UPDATE)) {
      issues.add(getContext().createConfigIssue(CDC.name(), CHANGE_TYPES, JDBC_94));
      return issues;
    }

    recordQueue = new LinkedBlockingQueue<>(2 * configBean.baseConfigBean.maxBatchSize);
    String container = configBean.pdb;

    databaseVersion = getDBVersion(issues);
    if (databaseVersion == -1) {
      return issues;
    }
    continuousMine = (databaseVersion < 19) && !configBean.disableContinuousMine;

    try {
      logMinerSession = new LogMinerSession.Builder(connection, databaseVersion)
          .setContinuousMine(continuousMine)
          .setDictionarySource(configBean.dictionary)
          .setDDLDictTracking(shouldTrackDDL)
          .setCommittedDataOnly(!useLocalBuffering)
          .setTablesForMining(configBean.baseConfigBean.schemaTableConfigs)
          .setTrackedOperations(configBean.baseConfigBean.changeTypes)
          .build();
    } catch (SQLException e) {
      LOG.error("Error while setting up a LogMiner session", e);
      issues.add(getContext().createConfigIssue(
          Groups.JDBC.name(),
          CONNECTION_STR,
          JDBC_00, hikariConfigBean.getConnectionString()
          )
      );
      return issues;
    }

    try {
      initializeStatements();
      alterSession();
    } catch (SQLException ex) {
      LOG.error("Error while creating statement", ex);
      issues.add(
          getContext().createConfigIssue(
              Groups.JDBC.name(),
              CONNECTION_STR,
              JDBC_00, hikariConfigBean.getConnectionString()
          )
      );
      return issues;
    }
    zoneId = ZoneId.of(configBean.dbTimeZone);
    dateTimeColumnHandler = new DateTimeColumnHandler(zoneId, configBean.convertTimestampToString);

    try {
      switch (configBean.startValue) {
        case SCN:
          BigDecimal scn = getEndingSCN();
          if (new BigDecimal(configBean.startSCN).compareTo(scn) > 0) {
            issues.add(
                getContext().createConfigIssue(CDC.name(), "oracleCDCConfigBean.startSCN", JDBC_47, scn.toPlainString()));
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

    initializeTableFilters();
    initializeTableSchemasCache(databaseVersion, issues);
    if (issues.size() > 0) {
      return issues;
    }

    if (useLocalBuffering && configBean.bufferLocation == BufferingValues.ON_DISK) {
      File tmpDir = new File(System.getProperty("java.io.tmpdir"));
      String relativePath =
          getContext().getSdcId() + "/" + getContext().getPipelineId() + "/" +
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
        parsingExecutor = Executors.newFixedThreadPool(
            configBean.parseThreadPoolSize,
            new ThreadFactoryBuilder().setNameFormat("Oracle CDC Origin Parse Thread - %d").build()
        );
      } else {
        parsingExecutor =
            Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("Oracle CDC Origin Parse Thread - %d").build()
        );
      }
    }

    if (configBean.txnWindow >= configBean.logminerWindow) {
      issues.add(getContext().createConfigIssue(Groups.CDC.name(), "oracleCDCConfigBean.logminerWindow", JDBC_81));
    }
    offsetVersion = useLocalBuffering ? OFFSET_VERSION_UNCOMMITTED : OFFSET_VERSION_STR;
    delay = getContext().createGauge("Read Lag (seconds)");
    return issues;
  }

  /**
   * Populates {@link OracleCDCSource#tableFilters} with the schema and table name patterns configured by the user.
   */
  private void initializeTableFilters() {
    for (SchemaTableConfigBean tables : configBean.baseConfigBean.schemaTableConfigs) {
      Pattern schemaPattern = createRegexFromSqlLikePattern(tables.schema);
      Pattern tablePattern = createRegexFromSqlLikePattern(tables.table);
      Pattern exclusionPattern = StringUtils.isEmpty(tables.excludePattern) ?
                                 null : Pattern.compile(tables.excludePattern);
      tableFilters.add(new TableFilter(schemaPattern, tablePattern, exclusionPattern));
    }
  }

  /**
   * Populates the table schemas cache with the existing database tables matching the table name filters configured
   * in the stage.
   *
   * Depending on the database version, it tries to connect to the configured pluggable database to
   * retrieve the info.
   *
   */
  private void initializeTableSchemasCache(int databaseVersion, List<ConfigIssue> issues) {
    try (Statement reusedStatement = connection.createStatement()) {
      // If version is 12+, then the check for table presence must be done in an alternate container!
      if (databaseVersion >= 12) {
        if (!StringUtils.isEmpty(configBean.pdb)) {
          String switchToPdb = "ALTER SESSION SET CONTAINER = " + configBean.pdb;
          try {
            reusedStatement.execute(switchToPdb);
          } catch (SQLException ex) {
            LOG.error("Error while switching to container: " + configBean.pdb, ex);
            issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(), USERNAME, JDBC_40, configBean.pdb));
            return;
          }
          containerized = true;
        }
      }

      BigDecimal lastSCN = null;
      try {
        lastSCN = getEndingSCN();
      } catch (SQLException ex) {
        LOG.warn("Could not retrieve the last SCN from database", ex);
      }

      for (SchemaTableConfigBean tables : configBean.baseConfigBean.schemaTableConfigs) {
        Pattern exclusionPattern = Pattern.compile(tables.excludePattern);
        try (ResultSet rs = jdbcUtil.getTableAndViewMetadata(connection, tables.schema, tables.table)) {
          while (rs.next()) {
            String schemaName = rs.getString(TABLE_METADATA_TABLE_SCHEMA_CONSTANT);
            String tableName = rs.getString(TABLE_METADATA_TABLE_NAME_CONSTANT);

            if (tables.excludePattern == null || !exclusionPattern.matcher(tableName).matches()) {
              SchemaAndTable table = new SchemaAndTable(schemaName.trim(), tableName.trim());
              try {
                tableSchemas.put(table, getTableSchema(table));
                if (lastSCN != null) {
                  tableSchemaLastUpdate.put(table, lastSCN);
                }
              } catch (SQLException ex) {
                LOG.error("Error while switching to container: " + configBean.pdb, ex);
                issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(), USERNAME, JDBC_50));
              }
            }
          }
        }
      }

      if (!issues.isEmpty()) {
        return;
      }

      if (databaseVersion >= 12) {
        try {
          switchContainer.execute();
          LOG.info("Switched to CDB$ROOT to start LogMiner.");
        } catch (SQLException ex) {
          // Fatal only if we switched to a PDB earlier
          if (containerized) {
            LOG.error("Error while switching to container: " + CDB_ROOT, ex);
            issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(), USERNAME, JDBC_40, CDB_ROOT));
            return;
          }
          // Log it anyway
          LOG.info("Switching containers failed, ignoring since there was no PDB switch", ex);
        }
      }

    } catch (SQLException ex) {
      LOG.error("Error while creating statement", ex);
      issues.add(
          getContext().createConfigIssue(
              Groups.JDBC.name(),
              CONNECTION_STR,
              JDBC_00, hikariConfigBean.getConnectionString()
          )
      );
    }
  }

  @VisibleForTesting
  Pattern createRegexFromSqlLikePattern(String pattern) {
    StringBuilder regex = new StringBuilder();
    StringBuilder token = new StringBuilder();

    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (c == '%' || c == '_') {
        if (token.length() > 0) {
          regex.append("\\Q" + token.toString() + "\\E");
          token.setLength(0);
        }
        if (c == '%') {
          regex.append(".*");
        } else {
          regex.append('.');
        }
      } else {
        token.append(c);
      }
    }
    if (token.length() > 0) {
      regex.append("\\Q" + token.toString() + "\\E");
    }

    return Pattern.compile(regex.toString());
  }

  @NotNull
  private LocalDateTime nowAtDBTz() {
    Timestamp ts;
    try (PreparedStatement query = connection.prepareStatement(GET_CURRENT_TIMESTAMP)) {
      query.setMaxRows(1);
      ResultSet rs = query.executeQuery();
      if (!rs.next()) {
        throw new StageException(JdbcErrors.JDBC_605, "No result returned");
      }
      ts = rs.getTimestamp(1);
    } catch (SQLException e) {
      throw new StageException(JdbcErrors.JDBC_605, e.getMessage());
    }
    return ts.toInstant().atZone(zoneId).toLocalDateTime();
  }

  private void initializeStatements() throws SQLException {
    getLatestSCN = connection.prepareStatement(CURRENT_SCN);
    dateStatement = connection.prepareStatement(NLS_DATE_FORMAT);
    tsStatement = connection.prepareStatement(NLS_TIMESTAMP_FORMAT);
    tsTzStatement = connection.prepareStatement(NLS_TIMESTAMP_TZ_FORMAT);
    numericFormat = connection.prepareStatement(NLS_NUMERIC_FORMAT);
    switchContainer = connection.prepareStatement(SWITCH_TO_CDB_ROOT);
    setSessionTimezone = connection.prepareStatement(SET_SESSION_TIMEZONE);
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

  private Map<String, Integer> getTableSchema(SchemaAndTable schemaAndTable) throws SQLException {
    Map<String, Integer> columns = new HashMap<>();
    String query = "SELECT * FROM \"" + schemaAndTable.getSchema() + "\".\"" + schemaAndTable.getTable() +
            "\" WHERE 1 = 0";
    try (Statement schemaStatement = connection.createStatement();
         ResultSet rs = schemaStatement.executeQuery(query)) {
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
    try (Statement statement = connection.createStatement();
         ResultSet versionSet = statement.executeQuery("SELECT version FROM product_component_version")) {
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
    generationExecutor.shutdown();
    try {
      generationExecutor.awaitTermination(5, TimeUnit.MINUTES);
    } catch (InterruptedException ex) {
      LOG.error("Interrupted while attempting to shutdown Generator thread", ex);
      Thread.currentThread().interrupt();
    }

    try {
      if (logMinerSession != null) {
        logMinerSession.close();
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

    return ParseUtil.generateField(
        logMinerSession.isEmptyStringEqualsNull(),
        column,
        columnValue,
        columnType,
        dateTimeColumnHandler
    );
  }

  private void alterSession() throws SQLException {
    if (containerized) {
      switchContainer.execute();
    }
    dateStatement.execute();
    tsStatement.execute();
    numericFormat.execute();
    tsTzStatement.execute();
    setSessionTimezone.execute();
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
              } catch(UnparseableSQLException ex) {
                unparseable.offer(x.sqlString);
              } catch (Exception ex) {
                LOG.error("Error while generating expired record from SQL: " + x.sqlString);
              }
              recordsDiscarded.incrementAndGet();
            }
          }
          txnDiscarded.incrementAndGet();
          HashQueue<RecordSequence> records = entry.getValue();
          if (records != null) {
            records.completeInserts();
            records.close();
          }
          iter.remove();
        }
      }
      LOG.info(Utils.format("Removed {} transactions and a total of {} records.",
          txnDiscarded.get(), recordsDiscarded.get()));
    } finally {
      bufferedRecordsLock.unlock();
    }
  }

  /**
   * An element is "expired" if the transaction started before the current window being processed
   * and if no records have actually been sent to the pipeline. If a record has been sent, then a commit was seen,
   * so it is not expired.
   * @param entry
   * @return
   */
  private boolean expired(Map.Entry<TransactionIdKey, HashQueue<RecordSequence>> entry, LocalDateTime startTime) {
    return startTime != null && // Can be null if starting from SCN and first batch is not complete yet.
        entry.getKey().txnStartTime.isBefore(startTime.minusSeconds(configBean.txnWindow)) &&
        (entry.getValue().isEmpty() || // Can be empty when we processed only "rollback=1" operations in this window.
         entry.getValue().peek().seq == 1);
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
      return configBean.bufferLocation == BufferingValues.IN_MEMORY ? new InMemoryHashQueue<>() :
          new FileBackedHashQueue<>(new File(txnBufferLocation, txnId));
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
      return o != null
          && o instanceof TransactionIdKey
          && this.txnId.equals(((TransactionIdKey) o).txnId);
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
      this.timestamp = version.equals(OFFSET_VERSION_UNCOMMITTED) ?
          LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(splits.next())), zoneId) : null;
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
      if ((version.equals(OFFSET_VERSION_UNCOMMITTED))) {
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

  private class TableFilter {
    final Pattern schemaPattern;
    final Pattern tablePattern;
    final Pattern exclusionPattern;

    public TableFilter(Pattern schemaPattern, Pattern tablePattern, Pattern exclusionPattern) {
      this.schemaPattern = schemaPattern;
      this.tablePattern = tablePattern;
      this.exclusionPattern = exclusionPattern;
    }
  }

}
