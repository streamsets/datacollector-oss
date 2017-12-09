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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.ChangeTypeValues;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean;
import com.zaxxer.hikari.HikariDataSource;
import net.jcip.annotations.GuardedBy;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plsql.plsqlLexer;
import plsql.plsqlParser;

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
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_00;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_16;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_40;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_41;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_42;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_43;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_44;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_47;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_48;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_49;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_50;
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
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.OPERATION;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.ROLLBACK_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.SELECT_FOR_UPDATE_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.UPDATE_CODE;
import static com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle.DateTimeColumnHandler.DT_FORMATTER;
import static com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle.Groups.CDC;
import static com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle.Groups.CREDENTIALS;

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
  private static final String GET_OLDEST_SCN =
      "SELECT FIRST_CHANGE#, STATUS from GV$ARCHIVED_LOG WHERE STATUS = 'A' ORDER BY FIRST_CHANGE#";
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
  private static final String TRYING_TO_START_LOG_MINER_WITH_START_DATE_AND_END_DATE =
      "Trying to start LogMiner with start date: {} and end date: {}";
  private static final String TRYING_TO_START_LOG_MINER_WITH_START_SCN_AND_END_SCN =
      "Trying to start LogMiner with start SCN: {} and end SCN: {}";
  private static final String START_TIME_END_TIME = "Start time = {}, End time = {}";
  private static final String XID = PREFIX + "xid";
  private static final String SEQ = "SEQ";
  private static final HashQueue<RecordSequence> EMPTY_LINKED_HASHSET = new InMemoryHashQueue<>(0);

  private static final String SENDING_TO_ERROR_AS_CONFIGURED = ". Sending to error as configured";
  private static final String UNSUPPORTED_TO_ERR = JDBC_85.getMessage() + SENDING_TO_ERROR_AS_CONFIGURED;
  private static final String DISCARDING_RECORD_AS_CONFIGURED = ". Discarding record as configured";
  private static final String UNSUPPORTED_DISCARD = JDBC_85.getMessage() + DISCARDING_RECORD_AS_CONFIGURED;
  private static final String UNSUPPORTED_SEND_TO_PIPELINE = JDBC_85.getMessage() + ". Sending to pipeline as configured";
  private static final String GENERATED_RECORD = "Generated Record: '{}'";
  public static final String FOUND_RECORDS_IN_TRANSACTION = "Found {} records in transaction";
  public static final String STARTED_LOG_MINER_WITH_START_SCN_AND_END_SCN = "Started LogMiner with start SCN: {} and end SCN: {}";
  public static final String REDO_SELECT_QUERY = "Redo select query for selectFromLogMnrContents = {}";
  public static final String CURRENT_LATEST_SCN_IS = "Current latest SCN is: {}";

  // https://docs.oracle.com/cd/E16338_01/appdev.112/e13995/constant-values.html#oracle_jdbc_OracleTypes_TIMESTAMPTZ
  private static final int TIMESTAMP_TZ_TYPE = -101;
  // https://docs.oracle.com/cd/E16338_01/appdev.112/e13995/constant-values.html#oracle_jdbc_OracleTypes_TIMESTAMPLTZ
  private static final int TIMESTAMP_LTZ_TYPE = -102;
  private DateTimeColumnHandler dateTimeColumnHandler;

  private boolean sentInitialSchemaEvent = false;
  private File txnBufferLocation;

  private PreparedStatement selectFromLogMnrContents;
  private static final int MISSING_LOG_FILE = 1291;
  private static final int QUERY_TIMEOUT = 1013;

  private final Lock bufferedRecordsLock = new ReentrantLock();
  private final BlockingQueue<StageException> stageExceptions = new LinkedBlockingQueue<>();

  @GuardedBy(value = "bufferedRecordsLock")
  private final Map<TransactionIdKey, HashQueue<RecordSequence>> bufferedRecords = new HashMap<>();

  private BlockingQueue<RecordOffset> recordQueue;

  private String selectString;
  private String version;
  private ZoneId zoneId;
  private boolean useLocalBuffering;

  private Gauge<Map<String, Object>> delay;
  private CallableStatement startLogMnrSCNToDate;

  private enum DDL_EVENT {
    CREATE,
    ALTER,
    DROP,
    TRUNCATE,
    STARTUP, // Represents event sent at startup.
    UNKNOWN
  }

  private static final Map<Integer, String> JDBCTypeNames = new HashMap<>();

  static {
    for (java.lang.reflect.Field jdbcType : Types.class.getFields()) {
      try {
        JDBCTypeNames.put((Integer) jdbcType.get(null), jdbcType.getName());
      } catch (Exception ex) {
        LOG.warn("JDBC Type Name access error", ex);
      }
    }
  }

  private static final String GET_TIMESTAMPS_FROM_LOGMNR_CONTENTS = "SELECT TIMESTAMP FROM V$LOGMNR_CONTENTS ORDER BY TIMESTAMP";
  private static final String OFFSET_DELIM = "::";
  private static final int RESULTSET_CLOSED_AS_LOGMINER_SESSION_CLOSED = 1306;
  private static final String NLS_DATE_FORMAT = "ALTER SESSION SET NLS_DATE_FORMAT = " + DateTimeColumnHandler.DT_SESSION_FORMAT;
  private static final String NLS_NUMERIC_FORMAT = "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = \'.,\'";
  private static final String NLS_TIMESTAMP_FORMAT =
      "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = " + DateTimeColumnHandler.TIMESTAMP_SESSION_FORMAT;
  private static final String NLS_TIMESTAMP_TZ_FORMAT =
      "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = " + DateTimeColumnHandler.ZONED_DATETIME_SESSION_FORMAT;
  private final Pattern ddlPattern = Pattern.compile("(CREATE|ALTER|DROP|TRUNCATE).*", Pattern.CASE_INSENSITIVE);

  private static final String TABLE_METADATA_TABLE_SCHEMA_CONSTANT = "TABLE_SCHEM";
  private static final String TABLE_METADATA_TABLE_NAME_CONSTANT = "TABLE_NAME";

  private final OracleCDCConfigBean configBean;
  private final HikariPoolConfigBean hikariConfigBean;
  private final Map<SchemaAndTable, Map<String, Integer>> tableSchemas = new HashMap<>();
  private final Map<SchemaAndTable, Map<String, String>> dateTimeColumns = new HashMap<>();
  private final Map<SchemaAndTable, Map<String, PrecisionAndScale>> decimalColumns = new HashMap<>();
  private final Map<SchemaAndTable, BigDecimal> tableSchemaLastUpdate = new HashMap<>();

  private final ExecutorService generationExecutor =
      Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Oracle CDC Data Generator").build());

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

  private final ParseTreeWalker parseTreeWalker = new ParseTreeWalker();
  private final SQLListener sqlListener = new SQLListener();

  public OracleCDCSource(HikariPoolConfigBean hikariConf, OracleCDCConfigBean oracleCDCConfigBean) {
    this.configBean = oracleCDCConfigBean;
    this.hikariConfigBean = hikariConf;
    this.shouldTrackDDL = configBean.dictionary == DictionaryValues.DICT_FROM_REDO_LOGS;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, final BatchMaker batchMaker) throws StageException {
    final int batchSize = Math.min(configBean.baseConfigBean.maxBatchSize, maxBatchSize);
    int recordGenerationAttempts = 0;
    boolean recordsProduced = false;
    String nextOffset = StringUtils.trimToEmpty(lastSourceOffset);
    pollForStageExceptions();
    while (!getContext().isStopped() && !recordsProduced && recordGenerationAttempts++ < MAX_RECORD_GENERATION_ATTEMPTS) {
      if (!sentInitialSchemaEvent) {
        for (SchemaAndTable schemaAndTable : tableSchemas.keySet()) {
          getContext().toEvent(
              createEventRecord(DDL_EVENT.STARTUP, null, schemaAndTable, ZERO, true));
        }
        sentInitialSchemaEvent = true;
      }

      try {
        if (!generationStarted) {
          startGeneratorThread(lastSourceOffset);
          generationStarted = true;
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

      for (int i = 0; i < batchSize; i++) {
        try {
          RecordOffset recordOffset = recordQueue.poll(1, TimeUnit.SECONDS);
          if (recordOffset != null) {
            if (recordOffset.record instanceof EventRecord) {
              getContext().toEvent((EventRecord) recordOffset.record);
            } else {
              batchMaker.addRecord(recordOffset.record);
              recordsProduced = true;
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
    pollForStageExceptions();
    return nextOffset;
  }

  private void pollForStageExceptions() throws StageException {
    StageException ex = stageExceptions.poll();
    if (ex != null) {
      throw ex;
    }
  }

  private void startGeneratorThread(String lastSourceOffset) throws StageException, SQLException {
    resetDBConnectionsIfRequired();
    Offset offset;
    LocalDateTime startTimestamp;
    startLogMnrForRedoDict();
    if (!StringUtils.isEmpty(lastSourceOffset)) {
      offset = new Offset(lastSourceOffset);
      if (lastSourceOffset.startsWith("v3")) {
        if (!useLocalBuffering) {
          throw new StageException(JDBC_82);
        }
        startTimestamp = offset.timestamp;
      } else {
        if (useLocalBuffering) {
          throw new StageException(JDBC_83);
        }
        startTimestamp = getDateForSCN(new BigDecimal(offset.scn));
        offset.timestamp = startTimestamp;
      }
      adjustStartTimeAndStartLogMnr(startTimestamp);
    } else { // reset the start date only if it not set.
      if (configBean.startValue != StartValues.SCN) {
        LocalDateTime startDate;
        if (configBean.startValue == StartValues.DATE) {
          startDate = LocalDateTime.parse(configBean.startDate, DT_FORMATTER);
        } else {
          startDate = nowAtDBTz();
        }
        startDate = adjustStartTimeAndStartLogMnr(startDate);
        offset = new Offset(version, startDate, ZERO, 0);
      } else {
        BigDecimal startCommitSCN = new BigDecimal(configBean.startSCN);
        startLogMnrSCNToDate.setBigDecimal(1, startCommitSCN);
        final LocalDateTime start = getDateForSCN(startCommitSCN);
        LocalDateTime endTime = getEndTimeForStartTime(start);
        startLogMnrSCNToDate.setString(2, endTime.format(DT_FORMATTER));
        startLogMnrSCNToDate.execute();
        offset = new Offset(version, start, startCommitSCN.toPlainString(), 0);
      }
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
  private LocalDateTime adjustStartTimeAndStartLogMnr(LocalDateTime startDate) throws SQLException {
    startDate = adjustStartTime(startDate);
    LocalDateTime endTime = getEndTimeForStartTime(startDate);
    startLogMinerUsingGivenDates(startDate.format(DT_FORMATTER),
        endTime.format(DT_FORMATTER));
    LOG.debug(START_TIME_END_TIME, startDate, endTime);
    return startDate;
  }

  private void resetDBConnectionsIfRequired() throws StageException, SQLException {
    if (dataSource == null || dataSource.isClosed()) {
      connection = null; // Force re-init without checking validity which takes time.
      dataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean);
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
      Offset startingOffset,
      PreparedStatement selectChanges
  ) {
    // When this is called the first time, Logminer was started either from SCN or from a start date, so we just keep
    // track of the start date etc.
    LOG.info("Attempting to generate records");
    boolean error = false;
    StringBuilder query = new StringBuilder();
    BigDecimal lastCommitSCN = new BigDecimal(startingOffset.scn);
    int sequenceNumber = startingOffset.sequence;
    LocalDateTime startTime = adjustStartTime(startingOffset.timestamp);
    LocalDateTime endTime = getEndTimeForStartTime(startTime);
    ResultSet resultSet = null;
    while (!getContext().isStopped()) {
      try {
        selectChanges = getSelectChangesStatement();
        if (!useLocalBuffering) {
          selectChanges.setBigDecimal(1, lastCommitSCN);
          selectChanges.setInt(2, sequenceNumber);
          selectChanges.setBigDecimal(3, lastCommitSCN);
          if (shouldTrackDDL) {
            selectChanges.setBigDecimal(4, lastCommitSCN);
          }
        }
        selectChanges.setQueryTimeout(configBean.queryTimeout);
        selectChanges.setFetchSize(configBean.jdbcFetchSize);
        resultSet = selectChanges.executeQuery();
        while (resultSet.next() && !getContext().isStopped()) {
          query.append(resultSet.getString(5));
          // CSF is 1 if the query is incomplete, so read the next row before parsing
          // CSF being 0 means query is complete, generate the record
          if (resultSet.getInt(9) == 0) {
            BigDecimal scnDecimal = resultSet.getBigDecimal(1);
            String scn = scnDecimal.toPlainString();
            String username = resultSet.getString(2);
            short op = resultSet.getShort(3);
            String timestamp = resultSet.getString(4);
            LocalDateTime tsDate = Timestamp.valueOf(timestamp).toLocalDateTime();
            delay.getValue().put("delay", getDelay(tsDate));
            String table = resultSet.getString(6);
            BigDecimal commitSCN = resultSet.getBigDecimal(7);
            String queryString = query.toString();
            query.setLength(0);
            int seq = resultSet.getInt(8);
            String xidUsn = String.valueOf(resultSet.getLong(10));
            String xidSlt = String.valueOf(resultSet.getString(11));
            String xidSqn = String.valueOf(resultSet.getString(12));
            String rsId = resultSet.getString(13);
            Object ssn = resultSet.getObject(14);
            String schema = String.valueOf(resultSet.getString(15));
            SchemaAndTable schemaAndTable = new SchemaAndTable(schema, table);
            String xid = xidUsn + "." + xidSlt + "." + xidSqn;
            TransactionIdKey key = new TransactionIdKey(xid);
            bufferedRecordsLock.lock();
            try {
              if (useLocalBuffering &&
                  bufferedRecords.containsKey(key) &&
                  bufferedRecords.get(key).contains(new RecordSequence(null, null, 0, 0, rsId, ssn, null))) {
                continue;
              }
            } finally {
              bufferedRecordsLock.unlock();
            }
            Offset offset = null;
            if (LOG.isDebugEnabled()) {
              LOG.debug("Commit SCN = {}, SCN = {}, Operation = {}, Redo SQL = {}", commitSCN, scn, op, queryString);
            }

            RuleContextAndOpCode ctxOp = null;
            try {
              ctxOp = getRuleContextAndCode(queryString, op);
            } catch (UnparseableSQLException ex) {
              try {
                errorRecordHandler.onError(JDBC_43, queryString);
              } catch (StageException stageException) {
                stageExceptions.add(stageException);
              }
              continue;
            }

            if (op != DDL_CODE && op != COMMIT_CODE && op != ROLLBACK_CODE) {
              if (!useLocalBuffering) {
                offset = new Offset(version, tsDate, commitSCN.toPlainString(), seq);
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
                Record record = generateRecord(attributes, ctxOp.operationCode, ctxOp.context);
                if (record != null && record.getEscapedFieldPaths().size() > 0) {
                  recordQueue.put(new RecordOffset(record, offset));
                }
              } else {
                bufferedRecordsLock.lock();
                try {
                  HashQueue<RecordSequence> records =
                      bufferedRecords.computeIfAbsent(key, x -> {
                        x.setTxnStartTime(tsDate);
                        return createTransactionBuffer(key.txnId);
                      });

                  int nextSeq = records.isEmpty() ? 1 : records.tail().seq + 1;
                  RecordSequence node =
                      new RecordSequence(attributes, queryString, nextSeq, ctxOp.operationCode, rsId, ssn, tsDate);
                  records.add(node);
                } finally {
                  bufferedRecordsLock.unlock();
                }
              }
            } else if (!getContext().isPreview() &&
                useLocalBuffering &&
                (op == COMMIT_CODE || op == ROLLBACK_CODE)) {
              // so this commit was previously processed or it is a rollback, so don't care.
              if (op == ROLLBACK_CODE || scnDecimal.compareTo(lastCommitSCN) <= 0) {
                bufferedRecordsLock.lock();
                try {
                  bufferedRecords.remove(key);
                } finally {
                  bufferedRecordsLock.unlock();
                }
              } else {
                bufferedRecordsLock.lock();
                try {
                  lastCommitSCN = scnDecimal;
                  int bufferedRecordsToBeRemoved = bufferedRecords.getOrDefault(key, EMPTY_LINKED_HASHSET).size();
                  LOG.debug(FOUND_RECORDS_IN_TRANSACTION, bufferedRecordsToBeRemoved);
                  addRecordsToQueue(scn, xid);
                } finally {
                  bufferedRecordsLock.unlock();
                }
              }
            } else {
              offset = new Offset(version, tsDate, scn, 0);
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
                recordQueue.put(new RecordOffset(
                    createEventRecord(type, queryString, schemaAndTable, offset.toString(), sendSchema), offset));
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
          stageExceptions.add(new StageException(JDBC_86, ex));
        } else if (ex.getErrorCode() != RESULTSET_CLOSED_AS_LOGMINER_SESSION_CLOSED) {
          LOG.warn("SQL Exception while retrieving records", ex);
        } else if (ex.getErrorCode() == QUERY_TIMEOUT) {
          LOG.warn("LogMiner select query timed out");
        } else {
          LOG.error("Error while reading data", ex);
          stageExceptions.add(new StageException(JDBC_52, ex));
        }
      } catch (StageException e) {
        LOG.error("Error while reading data", e);
        error = true;
        stageExceptions.add(e);
      } catch (InterruptedException ex) {
        LOG.error("Interrupted while waiting to add data");
        Thread.currentThread().interrupt();
      } catch (Exception ex) {
        LOG.error("Error while reading data", ex);
        error = true;
        stageExceptions.add(new StageException(JDBC_52, ex));
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
          endLogMnr.execute();
          if (!error) {
            discardOldUncommitted(startTime);
            startTime = adjustStartTime(endTime);
            endTime = getEndTimeForStartTime(startTime);
          }
          startLogMinerUsingGivenDates(startTime.format(DT_FORMATTER), endTime.format(DT_FORMATTER));
        } catch (SQLException ex) {
          LOG.error("Error while attempting to start LogMiner", ex);
          try {
            errorRecordHandler.onError(JDBC_52, ex);
          } catch (StageException e) {
            stageExceptions.add(e);
          }
        }
      }
    }
  }

  private LocalDateTime adjustStartTime(LocalDateTime startTime) {
    return useLocalBuffering ? startTime : startTime.minusSeconds(configBean.txnWindow);
  }


  private Record generateRecord(Map<String, String> attributes, int operationCode, ParserRuleContext ruleContext)
      throws StageException {
    String operation;
    SchemaAndTable table = new SchemaAndTable(attributes.get(SCHEMA), attributes.get(TABLE));
    operation = OperationType.getLabelFromIntCode(operationCode);
    attributes.put(OperationType.SDC_OPERATION_TYPE, String.valueOf(operationCode));
    attributes.put(OPERATION, operation);
    // Walk it and attach our sqlListener
    sqlListener.reset();
    if (configBean.allowNulls && !table.isNotEmpty()) {
      sqlListener.setColumns(tableSchemas.get(table).keySet());
    }

    parseTreeWalker.walk(sqlListener, ruleContext);

    Map<String, String> columns = sqlListener.getColumns();
    String rowId = columns.get(ROWID);
    columns.remove(ROWID);
    if (rowId != null) {
      attributes.put(ROWID_KEY, rowId);
    }
    Map<String, Field> fields = new HashMap<>();
    String id = useLocalBuffering ?
        attributes.get(RS_ID) + OFFSET_DELIM + attributes.get(SSN) :
        attributes.get(SCN) + OFFSET_DELIM + attributes.get(SEQ);
    Record record = getContext().createRecord(id);
    List<UnsupportedFieldTypeException> fieldTypeExceptions = new ArrayList<>();
    for (Map.Entry<String, String> column : columns.entrySet()) {
      String columnName = column.getKey();
      try {
        fields.put(columnName, objectToField(table, columnName, column.getValue()));
      } catch (UnsupportedFieldTypeException ex) {
        if (configBean.sendUnsupportedFields) {
          fields.put(columnName, Field.create(column.getValue()));
        }
        fieldTypeExceptions.add(ex);
      }
      if (decimalColumns.containsKey(table) && decimalColumns.get(table).containsKey(columnName)) {
        int precision = decimalColumns.get(table).get(columnName).precision;
        int scale = decimalColumns.get(table).get(columnName).scale;
        attributes.put("jdbc." + columnName + ".precision", String.valueOf(precision));
        attributes.put("jdbc." + columnName + ".scale", String.valueOf(scale));
      }
    }
    attributes.forEach((k, v) -> record.getHeader().setAttribute(k, v));
    record.set(Field.create(fields));
    LOG.debug(GENERATED_RECORD, record);
    Joiner errorStringJoiner = Joiner.on(", ");
    List<String> errorColumns = Collections.emptyList();
    if (!fieldTypeExceptions.isEmpty()) {
      errorColumns = fieldTypeExceptions.stream().map(ex -> {
            String fieldTypeName = JDBCTypeNames.getOrDefault(ex.fieldType, "unknown");
            return "[Column = '" + ex.column + "', Type = '" + fieldTypeName + "', Value = '" + ex.columnVal + "']";
          }
      ).collect(Collectors.toList());
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
  }

  private boolean handleUnsupportedFieldTypes(Record r, String error) {
    switch (configBean.unsupportedFieldOp) {
      case SEND_TO_PIPELINE:
        LOG.warn(Utils.format(UNSUPPORTED_SEND_TO_PIPELINE, error, r.getHeader().getAttribute(TABLE)));
        return true;
      case TO_ERROR:
        LOG.warn(Utils.format(UNSUPPORTED_TO_ERR, error, r.getHeader().getAttribute(TABLE)));
        getContext().toError(r, JDBC_85, error, r.getHeader().getAttribute(TABLE));
        return false;
      case DISCARD:
        LOG.warn(Utils.format(UNSUPPORTED_DISCARD, error, r.getHeader().getAttribute(TABLE)));
        return false;
      default:
        throw new IllegalStateException("Unknown Record Handling option");
    }
  }

  private long localDateTimeToEpoch(LocalDateTime date) {
    return date.atZone(zoneId).toEpochSecond();
  }

  private void addRecordsToQueue(
      String commitScn,
      String xid
  ) throws StageException, ParseException, InterruptedException {
    TransactionIdKey key = new TransactionIdKey(xid);
    bufferedRecordsLock.lock();
    try {
      HashQueue<RecordSequence> records = bufferedRecords.getOrDefault(key, EMPTY_LINKED_HASHSET);
      records.completeInserts();
      while (!records.isEmpty()) {
        RecordSequence r = records.remove();
        try {
          if (configBean.keepOriginalQuery) {
            r.headers.put(QUERY_KEY, r.sqlString);
          }
          RuleContextAndOpCode ctxOp = getRuleContextAndCode(r.sqlString, r.opCode);
          Record record = generateRecord(r.headers, ctxOp.operationCode, ctxOp.context);
          if (record != null && record.getEscapedFieldPaths().size() > 0) {
            recordQueue.put(
                new RecordOffset(record, new Offset(VERSION_UNCOMMITTED, r.timestamp, commitScn, r.seq)));
          }
        } catch (UnparseableSQLException ex) {
          try {
            errorRecordHandler.onError(JDBC_43, r.sqlString);
          } catch (StageException stageException) {
            stageExceptions.add(stageException);
          }
        }
      }
      records.close();
      bufferedRecords.remove(key);
    } finally {
      bufferedRecordsLock.unlock();
    }
  }

  private EventRecord createEventRecord(
      DDL_EVENT type,
      String redoSQL,
      SchemaAndTable schemaAndTable,
      String scnSeq,
      boolean sendSchema
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

    if (LOG.isDebugEnabled()) {
      LOG.debug("Event produced: " + event);
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
      if (!tableSchemaLastUpdate.containsKey(schemaAndTable) || scnDecimal.compareTo(tableSchemaLastUpdate.get(schemaAndTable)) > 0) {
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
        "SCN: '{}' is not valid and cannot be found in LogMiner logs", commitSCN.toPlainString()));
  }

  private LocalDateTime getEndTimeForStartTime(LocalDateTime startTime) {
    LocalDateTime sessionMax = startTime.plusSeconds(configBean.logminerWindow);
    LocalDateTime now = nowAtDBTz();
    return (sessionMax.isAfter(now) ? now : sessionMax);
  }

  private void startLogMinerUsingGivenSCNs(BigDecimal oldestSCN, BigDecimal endSCN) throws SQLException {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug(TRYING_TO_START_LOG_MINER_WITH_START_SCN_AND_END_SCN,
            oldestSCN.toPlainString(), endSCN.toPlainString());
      }
      startLogMnrForCommitSCN.setBigDecimal(1, oldestSCN);
      startLogMnrForCommitSCN.setBigDecimal(2, endSCN);
      startLogMnrForCommitSCN.execute();
      if (LOG.isDebugEnabled()) {
        LOG.debug(STARTED_LOG_MINER_WITH_START_SCN_AND_END_SCN,
            oldestSCN.toPlainString(), endSCN.toPlainString());
      }
    } catch (SQLException ex) {
      LOG.debug("SQLException while starting LogMiner", ex);
      throw ex;
    }
  }

  private void startLogMinerUsingGivenDates(String startDate, String endDate) throws SQLException {
    try {
      LOG.info(TRYING_TO_START_LOG_MINER_WITH_START_DATE_AND_END_DATE, startDate, endDate);
      startLogMnrForData.setString(1, startDate);
      startLogMnrForData.setString(2, endDate);
      startLogMnrForData.execute();
    } catch (SQLException ex) {
      LOG.debug("SQLException while starting LogMiner", ex);
      throw ex;
    }
  }


  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
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
        dataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean);
        connection = dataSource.getConnection();
        connection.setAutoCommit(false);
      } catch (StageException | SQLException e) {
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
      issues.add(
          getContext().createConfigIssue(
              Groups.JDBC.name(),
              CONNECTION_STR,
              JDBC_00,
              hikariConfigBean.connectionString
          )
      );
    }
    zoneId = ZoneId.of(configBean.dbTimeZone);
    dateTimeColumnHandler = new DateTimeColumnHandler(zoneId);
    String commitScnField;
    BigDecimal scn = null;
    try {
      scn = getEndingSCN();
      switch (configBean.startValue) {
        case SCN:
          if (new BigDecimal(configBean.startSCN).compareTo(scn) > 0) {
            issues.add(
                getContext().createConfigIssue(CDC.name(), "oracleCDCConfigBean.startSCN", JDBC_47, scn.toPlainString()));
          }
          break;
        case LATEST:
          // If LATEST is used, use now() as the startDate and proceed as if a startDate was specified
          configBean.startDate = nowAtDBTz().format(DT_FORMATTER);
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
        Pattern p = StringUtils.isEmpty(tables.excludePattern) ? null : Pattern.compile(tables.excludePattern);

        for (String table : tables.tables) {
          try (ResultSet rs =
                   JdbcUtil.getTableMetadata(connection, null, tables.schema, table, true)) {
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
      issues.add(
          getContext().createConfigIssue(
              Groups.JDBC.name(),
              CONNECTION_STR,
              JDBC_00,
              hikariConfigBean.connectionString
          )
      );
      return issues;
    }

    final String ddlTracking = shouldTrackDDL ? " + DBMS_LOGMNR.DDL_DICT_TRACKING" : "";

    final String readCommitted = useLocalBuffering ? "" : "+ DBMS_LOGMNR.COMMITTED_DATA_ONLY";

    this.logMinerProcedure = "BEGIN"
        + " DBMS_LOGMNR.START_LOGMNR("
        + " {},"
        + " {},"
        + " OPTIONS => DBMS_LOGMNR." + configBean.dictionary.name()
        + "          + DBMS_LOGMNR.CONTINUOUS_MINE"
        + readCommitted
        + "          + DBMS_LOGMNR.NO_SQL_DELIMITER"
        + ddlTracking
        + ");"
        + " END;";

    final String base =
        "SELECT SCN, USERNAME, OPERATION_CODE, TIMESTAMP, SQL_REDO, TABLE_NAME, " + commitScnField +
            ", SEQUENCE#, CSF, XIDUSN, XIDSLT, XIDSQN, RS_ID, SSN, SEG_OWNER " +
            " FROM V$LOGMNR_CONTENTS" +
            " WHERE ";

    final String tableCondition = getListOfSchemasAndTables(schemasAndTables);

    final String commitRollbackCondition = Utils.format("OPERATION_CODE = {} OR OPERATION_CODE = {}",
        COMMIT_CODE, ROLLBACK_CODE);

    final String operationsCondition = "OPERATION_CODE IN (" + getSupportedOperations() + ")";

    final String restartNonBufferCondition = Utils.format("((" + commitScnField + " = ? AND SEQUENCE# > ?) OR "
        + commitScnField + "  > ?)" + (shouldTrackDDL ? " OR (OPERATION_CODE = {} AND SCN > ?)" : ""), DDL_CODE);


    if (useLocalBuffering) {
      selectString = String
          .format("%s ((%s AND (%s)) OR (%s))", base, tableCondition, operationsCondition, commitRollbackCondition);
    } else {
      selectString = base +
          " (" + tableCondition + " AND (" + operationsCondition + "))" + "AND (" + restartNonBufferCondition + ")";
    }

    try {
      initializeLogMnrStatements();
    } catch (SQLException ex) {
      LOG.error("Error while creating statement", ex);
      issues.add(
          getContext().createConfigIssue(
              Groups.JDBC.name(),
              CONNECTION_STR,
              JDBC_00,
              hikariConfigBean.connectionString
          )
      );
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

    if (configBean.baseConfigBean.caseSensitive) {
      sqlListener.setCaseSensitive();
    }

    if (configBean.allowNulls) {
      sqlListener.allowNulls();
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
   * @param schemaAndTables List of SchemaAndTable objects
   * @return SQL string of schemas and tables
   */
  private String getListOfSchemasAndTables(List<SchemaAndTable> schemaAndTables) {
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
      queries.add(Utils.format(
          "(SEG_OWNER='{}' AND TABLE_NAME IN ({}))", entry.getKey(), formatTableList(entry.getValue())));
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
    BigDecimal endSCN = getEndingSCN();

    SQLException lastException = null;
    boolean startedLogMiner = false;

    try (ResultSet rs = getOldestSCN.executeQuery()) {
      while (rs.next()) {
        BigDecimal oldestSCN = rs.getBigDecimal(1);
        try {
          startLogMinerUsingGivenSCNs(oldestSCN, endSCN);
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
    startLogMnrForCommitSCN = connection.prepareCall(
        Utils.format(logMinerProcedure, "STARTSCN => ?", "ENDSCN => ?"));
    startLogMnrForData = connection.prepareCall(
        Utils.format(logMinerProcedure, "STARTTIME => ?", "ENDTIME => ?"));
    startLogMnrSCNToDate = connection.prepareCall(
        Utils.format(logMinerProcedure, "STARTSCN => ?", "ENDTIME => ?"));
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

  private void validateTablePresence(Statement statement, List<SchemaAndTable> schemaAndTables,
                                     List<ConfigIssue> issues) {
    for (SchemaAndTable schemaAndTable : schemaAndTables) {
      try {
        statement.execute("SELECT * FROM \"" + schemaAndTable.getSchema() + "\".\"" + schemaAndTable.getTable() +
                "\" WHERE 1 = 0");
      } catch (SQLException ex) {
        StringBuilder sb = new StringBuilder("Table: ").append(schemaAndTable).append(" does not exist.");
        if (StringUtils.isEmpty(configBean.pdb)) {
          sb.append(" PDB was not specified. If the database was created inside a PDB, please specify PDB");
        }
        LOG.error(sb.toString(), ex);
        issues.add(getContext().createConfigIssue(Groups.CDC.name(), "oracleCDCConfigBean.baseConfigBean.tables",
                JDBC_16, schemaAndTable));
      }
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
      if (endLogMnr != null && !endLogMnr.isClosed())
        endLogMnr.execute();
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

    Field field;
    // All types as of JDBC 2.0 are here:
    // https://docs.oracle.com/javase/8/docs/api/constant-values.html#java.sql.Types.ARRAY
    // Good source of recommended mappings is here:
    // http://www.cs.mun.ca/java-api-1.5/guide/jdbc/getstart/mapping.html
    columnValue = NULL.equalsIgnoreCase(columnValue) ? null : columnValue; //NOSONAR
    switch (columnType) {
      case Types.BIGINT:
        field = Field.create(Field.Type.LONG, columnValue);
        break;
      case Types.BINARY:
      case Types.LONGVARBINARY:
      case Types.VARBINARY:
        field = Field.create(Field.Type.BYTE_ARRAY, RawTypeHandler.parseRaw(column, columnValue, columnType));
        break;
      case Types.BIT:
      case Types.BOOLEAN:
        field = Field.create(Field.Type.BOOLEAN, columnValue);
        break;
      case Types.CHAR:
      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.VARCHAR:
        field = Field.create(Field.Type.STRING, columnValue);
        break;
      case Types.DECIMAL:
      case Types.NUMERIC:
        field = Field.create(Field.Type.DECIMAL, columnValue);
        break;
      case Types.DOUBLE:
        field = Field.create(Field.Type.DOUBLE, columnValue);
        break;
      case Types.FLOAT:
      case Types.REAL:
        field = Field.create(Field.Type.FLOAT, columnValue);
        break;
      case Types.INTEGER:
        field = Field.create(Field.Type.INTEGER, columnValue);
        break;
      case Types.SMALLINT:
      case Types.TINYINT:
        field = Field.create(Field.Type.SHORT, columnValue);
        break;
      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        // For whatever reason, Oracle returns all the date/time/timestamp fields as the same type, so additional
        // logic is required to accurately parse the type
        String actualType = dateTimeColumns.get(schemaAndTable).get(column);
        field = dateTimeColumnHandler.getDateTimeStampField(column, columnValue, columnType, actualType);
        break;
      case Types.TIMESTAMP_WITH_TIMEZONE:
      case TIMESTAMP_TZ_TYPE:
        field = dateTimeColumnHandler.getTimestampWithTimezoneField(columnValue);
        break;
      case TIMESTAMP_LTZ_TYPE:
        field = dateTimeColumnHandler.getTimestampWithLocalTimezone(columnValue);
        break;
      case Types.ROWID:
      case Types.CLOB:
      case Types.NCLOB:
      case Types.BLOB:
      case Types.ARRAY:
      case Types.DATALINK:
      case Types.DISTINCT:
      case Types.JAVA_OBJECT:
      case Types.NULL:
      case Types.OTHER:
      case Types.REF:
      case Types.REF_CURSOR:
      case Types.SQLXML:
      case Types.STRUCT:
      case Types.TIME_WITH_TIMEZONE:
      default:
        throw new UnsupportedFieldTypeException(column, columnValue, columnType);
    }

    return field;
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
                RuleContextAndOpCode ctxOp = getRuleContextAndCode(x.sqlString, x.opCode);
                Record record = generateRecord(x.headers, ctxOp.operationCode, ctxOp.context);
                if (record != null) {
                  getContext().toError(record, JDBC_84, entry.getKey().txnId, entry.getKey().txnStartTime);
                }
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

  private RuleContextAndOpCode getRuleContextAndCode(String queryString, int op) throws UnparseableSQLException {
    plsqlLexer lexer = new plsqlLexer(new ANTLRInputStream(queryString));
    CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    plsqlParser parser = new plsqlParser(tokenStream);
    RuleContextAndOpCode contextAndOpCode = new RuleContextAndOpCode();
    switch (op) {
      case UPDATE_CODE:
      case SELECT_FOR_UPDATE_CODE:
        contextAndOpCode.context = parser.update_statement();;
        contextAndOpCode.operationCode = OperationType.UPDATE_CODE;
        break;
      case INSERT_CODE:
        contextAndOpCode.context = parser.insert_statement();
        contextAndOpCode.operationCode = OperationType.INSERT_CODE;
        break;
      case DELETE_CODE:
        contextAndOpCode.context = parser.delete_statement();
        contextAndOpCode.operationCode = OperationType.DELETE_CODE;
        break;
      case DDL_CODE:
      case COMMIT_CODE:
      case ROLLBACK_CODE:
        break;
      default:
        throw new UnparseableSQLException(queryString);
    }
    return contextAndOpCode;
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

  private class PrecisionAndScale {
    int precision;
    int scale;

    PrecisionAndScale(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
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

    final int sequence;

    public Offset(String version, LocalDateTime timestamp, String scn, int sequence) {
      this.version = version;
      this.scn = scn;
      this.timestamp = timestamp;
      this.sequence = sequence;
    }

    public Offset(String offsetString) {
      int index = 0;
      String[] splits = offsetString.split(OFFSET_DELIM);
      this.version = splits[index++];
      this.timestamp =
          version.equals(VERSION_UNCOMMITTED) ?
              LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(splits[index++])), zoneId) : null;
      this.scn = splits[index++];
      this.sequence = Integer.parseInt(splits[index]);
    }

    public String toString() {
      return version + OFFSET_DELIM +
          (version.equals(VERSION_UNCOMMITTED) ? localDateTimeToEpoch(timestamp) + OFFSET_DELIM : "") +
          scn + OFFSET_DELIM +
          sequence;
    }
  }

  private class RuleContextAndOpCode {
    ParserRuleContext context;
    int operationCode;
  }

  private class RecordOffset {
    final Record record;
    final Offset offset;

    public RecordOffset(Record record, Offset offset) {
      this.record = record;
      this.offset = offset;
    }
  }

  private class UnparseableSQLException extends Exception {
    final String sql;

    UnparseableSQLException(String sql) {
      this.sql = sql;
    }
  }

}
