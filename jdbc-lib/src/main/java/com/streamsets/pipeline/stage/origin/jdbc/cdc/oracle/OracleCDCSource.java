/**
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
import com.zaxxer.hikari.HikariDataSource;
import net.jcip.annotations.GuardedBy;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plsql.plsqlLexer;
import plsql.plsqlParser;

import java.math.BigDecimal;
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
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_00;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_16;
import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_37;
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
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.COMMIT_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.DDL_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.DELETE_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.INSERT_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.OPERATION;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.ROLLBACK_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.SELECT_FOR_UPDATE_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.UPDATE_CODE;
import static com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle.Groups.CDC;
import static com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle.Groups.CREDENTIALS;

public class OracleCDCSource extends BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(OracleCDCSource.class);
  private static final String CDB_ROOT = "CDB$ROOT";
  private static final String HIKARI_CONFIG_PREFIX = "hikariConf.";
  private static final String DRIVER_CLASSNAME = HIKARI_CONFIG_PREFIX + "driverClassName";
  private static final String USERNAME = HIKARI_CONFIG_PREFIX + "username";
  private static final String CONNECTION_STR = HIKARI_CONFIG_PREFIX + "connectionString";
  private static final String CURRENT_SCN = "SELECT CURRENT_SCN FROM V$DATABASE";
  // At the time of executing this statement, either the cachedSCN is 0
  // (which means we are executing for the first time), or it is no longer valid, so select
  // only the ones that are > than the cachedSCN.
  private static final String GET_OLDEST_SCN =
      "SELECT FIRST_CHANGE#, STATUS from V$ARCHIVED_LOG WHERE STATUS = 'A' AND FIRST_CHANGE# > ? ORDER BY FIRST_CHANGE#";
  private static final String SWITCH_TO_CDB_ROOT = "ALTER SESSION SET CONTAINER = CDB$ROOT";
  private static final String PREFIX = "oracle.cdc.";
  private static final String SCN = PREFIX + "scn";
  private static final String USER = PREFIX + "user";
  private static final String DDL_TEXT = PREFIX + "ddl";
  private static final String DATE = "DATE";
  private static final String TIME = "TIME";
  private static final String TIMESTAMP = "TIMESTAMP";
  private static final String TIMESTAMP_HEADER = PREFIX + TIMESTAMP.toLowerCase();
  private static final String TABLE = PREFIX + "table";
  private static final String NULL = "NULL";
  private static final String VERSION_STR = "v2";
  private static final String VERSION_UNCOMMITTED = "v3";
  private static final String ZERO = "0";
  private static final int ONE_HOUR = 60 * 60 * 1000;

  // What are all these constants?
  // String templates used in debug logging statements. To avoid unnecessarily creating new strings,
  // we just reuse these constants to avoid adding an if(LOG.isDebugEnabled()) call.
  private static final String START_DATE_REFRESH_NOT_REQUIRED = "Start date refresh not required";
  private static final String START_DATE_REFRESHED_TO = "Start date refreshed to: '{}'";
  private static final String TRYING_TO_START_LOG_MINER_WITH_START_DATE_AND_END_DATE =
      "Trying to start LogMiner with start date: {} and end date: {}";
  private static final String TRYING_TO_START_LOG_MINER_WITH_START_SCN_AND_END_SCN =
      "Trying to start LogMiner with start SCN: {} and end SCN: {}";
  private static final String COUNT_FOR_BATCH_AND_BATCH_SIZE = "Count for batch = {} and batch size = {}";
  private static final String START_TIME_END_TIME = "Start time = {}, End time = {}";
  private static final String STARTING_COMMIT_SCN_ROWS_SKIPPED = "Starting Commit SCN = {}, Rows skipped = {}";
  public static final String XID = PREFIX + "xid";
  public static final String SEQ = "SEQ";
  private static final HashQueue<RecordSequence> EMPTY_LINKED_HASHSET = new HashQueue<>(0);
  public static final String ROWID = "ROWID";

  private boolean sentInitialSchemaEvent = false;
  private Optional<ResultSet> currentResultSet = Optional.empty(); //NOSONAR
  private Optional<String> currentCommitTxn = Optional.empty();

  // These two are required for restarting an origin
  private BigDecimal resumeCommitSCN = BigDecimal.ZERO;
  private int resumeSeq = 0;

  private PreparedStatement resume;
  private PreparedStatement start;
  private static final int MISSING_LOG_FILE = 1291;

  private final Lock bufferedRecordsLock = new ReentrantLock();

  @GuardedBy(value = "bufferedRecordsLock")
  private final Map<TransactionIdKey, HashQueue<RecordSequence>> bufferedRecords = new HashMap<>();
  private String startString;
  private String resumeString;

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

  private static final String GET_COMMIT_TS = "SELECT COMMIT_TIMESTAMP FROM V$LOGMNR_CONTENTS";
  private static final String OFFSET_DELIM = "::";
  private static final int RESULTSET_CLOSED_AS_LOGMINER_SESSION_CLOSED = 1306;
  private static final String NLS_DATE_FORMAT = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MM-YYYY HH24:MI:SS'";
  private static final String NLS_NUMERIC_FORMAT = "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = \'.,\'";
  private static final String NLS_TIMESTAMP_FORMAT =
      "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'";
  private final Pattern toDatePattern = Pattern.compile("TO_DATE\\('(.*)',.*");
  // If a date is set into a timestamp column (or a date field is widened to a timestamp,
  // a timestamp ending with "." is returned (like 2016-04-15 00:00:00.), so we should also ignore the trailing ".".
  private final Pattern toTimestampPattern = Pattern.compile("TO_TIMESTAMP\\('(.*[^\\.]).*'");
  private final Pattern ddlPattern = Pattern.compile("(CREATE|ALTER|DROP|TRUNCATE).*", Pattern.CASE_INSENSITIVE);
  private final DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

  private final OracleCDCConfigBean configBean;
  private final HikariPoolConfigBean hikariConfigBean;
  private final Properties driverProperties = new Properties();
  private final Map<String, Map<String, Integer>> tableSchemas = new HashMap<>();
  private final Map<String, Map<String, String>> dateTimeColumns = new HashMap<>();
  private final Map<String, Map<String, PrecisionAndScale>> decimalColumns = new HashMap<>();
  private final Map<String, BigDecimal> tableSchemaLastUpdate = new HashMap<>();
  private final AtomicReference<Offset> offsetReference = new AtomicReference<>(null);
  private final ExecutorService resultSetExecutor =
      Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Oracle CDC Record Generator").build());
  private final ScheduledExecutorService discardExecutor =
      Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Oracle CDC Old Record Discarder")
          .build());
  private Future<?> resultSetClosingFuture = null;
  private final boolean shouldTrackDDL;

  private String logMinerProcedure;
  private ErrorRecordHandler errorRecordHandler;
  private boolean containerized = false;
  private long lastRefresh = 0;
  private BigDecimal scnAtLastRefresh = BigDecimal.ZERO;
  private BigDecimal lastSCN = null;
  private LocalDateTime lastSCNTimestamp = null;
  private LocalDateTime startDate;
  private LocalDateTime lastEndTime;
  private volatile boolean incompleteBatch = false;
  private volatile boolean refreshed = false;

  private HikariDataSource dataSource = null;
  private Connection connection = null;
  private PreparedStatement getOldestSCN;
  private PreparedStatement getLatestSCN;
  private CallableStatement startLogMnrForCommitSCN;
  private CallableStatement startLogMnrForData;
  private CallableStatement startLogMnrForResume;
  private CallableStatement endLogMnr;
  private PreparedStatement dateStatement;
  private PreparedStatement tsStatement;
  private PreparedStatement numericFormat;
  private PreparedStatement switchContainer;
  private PreparedStatement getCommitTimestamp;

  private final ParseTreeWalker parseTreeWalker = new ParseTreeWalker();
  private final SQLListener sqlListener = new SQLListener();

  public OracleCDCSource(HikariPoolConfigBean hikariConf, OracleCDCConfigBean oracleCDCConfigBean) {
    this.configBean = oracleCDCConfigBean;
    this.hikariConfigBean = hikariConf;
    this.shouldTrackDDL = configBean.dictionary == DictionaryValues.DICT_FROM_REDO_LOGS;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, final BatchMaker batchMaker) throws StageException {
    boolean recordsProduced = false;
    final int batchSize = Math.min(configBean.baseConfigBean.maxBatchSize, maxBatchSize);
    while (!getContext().isStopped() && !recordsProduced) {
      if (currentCommitTxn.isPresent()) {
        Offset os = new Offset(lastSourceOffset);
        bufferedRecordsLock.lock();
        try {
          Iterator<RecordSequence> txn = bufferedRecords.get(new TransactionIdKey(currentCommitTxn.get())).iterator();
          while (txn.hasNext() && txn.next().seq <= os.sequence) {
            txn.remove();
          }
          int newSeq = addRecordsToBatch(batchSize, batchMaker, currentCommitTxn.get());
          recordsProduced = newSeq > 0;
          resumeSeq = Math.max(os.sequence, newSeq);
        } finally {
          bufferedRecordsLock.unlock();
        }
        // This is used when a statement that is not a commit or rollback comes in. So make sure the actual value is set
        offsetReference.set(new Offset(os.version, os.timestamp, os.scn, resumeSeq));
        // Only the final count changes, since addRecordToBatch only reads from same txn.
        continue;
      }
      if (!sentInitialSchemaEvent) {
        for (String table : tableSchemas.keySet()) {
          getContext().toEvent(
              createEventRecord(DDL_EVENT.STARTUP, null, table, ZERO, true));
        }
        sentInitialSchemaEvent = true;
      }

      // Sometimes even though the SCN number has been updated, the select won't return the latest changes for a bit,
      // because the view gets materialized only on calling the SELECT - so the executeQuery may not return anything.
      // To avoid missing data in such cases, we return the new SCN only when we actually read data.
      PreparedStatement selectChanges = null;
      final Semaphore generationSema = new Semaphore(0);
      final AtomicBoolean generationStarted = new AtomicBoolean(false);
      try {
        if (dataSource == null || dataSource.isClosed()) {
          connection = null; // Force re-init without checking validity which takes time.
          dataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean, driverProperties);
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

        String lastOffset;
        boolean unversioned = true;
        long rowsRead;
        boolean closeResultSet = false;
        if (!StringUtils.isEmpty(lastSourceOffset)) {
          Offset offset = new Offset(lastSourceOffset);
          selectChanges = resume;
          // unversioned offsets: scn::nurowsread
          // so if the offset is unversioned, just pick up the next commit_scn and don't skip any rows so we get all
          // actions from the following commit onwards.
          // This may cause duplicates when upgrading from unversioned to v2/v3 offsets,
          // but this allows us to handle out of order commits.
          if (lastSourceOffset.startsWith("v")) {
            unversioned = false;
          }
          if (lastSourceOffset.startsWith("v3")) {
            if (!configBean.bufferLocally) {
              throw new StageException(JDBC_82);
            }
            //v3 offsets are of the form: v3::commit_timestamp_start_millis::commit_scn::numrowsread.
            startDate = offset.timestamp;
            resumeCommitSCN = new BigDecimal(offset.scn);
            resumeSeq = offset.sequence;
            if (lastSCN != null) {
              // The lastSCN is actually SCN for the last statement, not necessarily a commit, so use that one
              // if it exists. On restart use the one from the offset.
              if (LOG.isDebugEnabled()) {
                LOG.debug(Utils.format(
                    "Starting Logminer from SCN: {} and end date: {}", lastSCN.toPlainString(), lastSCNTimestamp));
              }
              startLogMnrForResume.setBigDecimal(1, lastSCN);
              startLogMnrForResume.setString(2,
                  getEndTimeForStartTime(lastSCNTimestamp).format(dtFormatter));
            }
            // If lastEndTime is not null, we have read records before and are buffered in memory, just read
            // from beginning of next window
            if (lastEndTime != null) {
              startDate = refreshStartDate(BigDecimal.ZERO);
            } else {
              // So this is starting from shutdown/failure, which means the start date is derived from the offset
              // Since that is the timestamp of the last commit we saw, we must look at the txn_window before that
              // to see any buffered records at time of failure
              startDate = startDate.minusSeconds(configBean.txnWindow);
            }
          } else {
            if (configBean.bufferLocally) {
              throw new StageException(JDBC_83);
            }
            // v2 are of the form : v2::commit_scn:numrowsread.
            lastOffset = offset.scn;
            rowsRead = offset.sequence;
            BigDecimal startCommitSCN = new BigDecimal(lastOffset);
            startDate = refreshStartDate(startCommitSCN);
            selectChanges.setBigDecimal(1, startCommitSCN);
            long rowsToSkip = unversioned ? 0 : rowsRead;
            selectChanges.setLong(2, rowsToSkip);
            selectChanges.setBigDecimal(3, startCommitSCN);
            if (shouldTrackDDL) {
              selectChanges.setBigDecimal(4, startCommitSCN);
            }
            LOG.debug(STARTING_COMMIT_SCN_ROWS_SKIPPED, startCommitSCN, rowsToSkip);
          }
        } else { // reset the start date only if it not set.
          selectChanges = start;
          if (startDate == null) {
            if (configBean.startValue != StartValues.SCN) {
              resumeCommitSCN = BigDecimal.ZERO;
              resumeSeq = 0;
              if (configBean.startValue == StartValues.DATE) {
                startDate = LocalDateTime.parse(configBean.startDate, dtFormatter);
              } else {
                startDate = LocalDateTime.now();
              }
              closeResultSet = true;
            } else {
              BigDecimal startCommitSCN = new BigDecimal(configBean.startSCN);
              startDate = refreshStartDate(startCommitSCN);
            }
          }
          if (lastEndTime != null) {
            startDate = refreshStartDate(BigDecimal.ZERO);
          }
        }

        try {
          if (refreshed) {
            startDate = startDate.minusSeconds(configBean.txnWindow);
          }
          LocalDateTime endTime = getEndTimeForStartTime(startDate);
          if (lastSCN == null || !configBean.bufferLocally) {
            startLogMinerUsingGivenDates(startDate.format(dtFormatter), endTime.format(dtFormatter));
          } else {
            startLogMnrForResume.execute();
          }
          LOG.debug(START_TIME_END_TIME, startDate, endTime);
          lastEndTime = endTime;
        } catch (SQLException ex) {
          LOG.error("Error while starting LogMiner", ex);
          throw new StageException(JDBC_52, ex);
        }

        final PreparedStatement select = selectChanges;
        final boolean closeRS = closeResultSet;
        resultSetClosingFuture = resultSetExecutor.submit(() -> {
          generationStarted.set(true);
          try {
            generateRecords(batchSize, select, batchMaker, closeRS);
          } catch (Exception ex) {
            LOG.error("Error while generating records", ex);
            Throwables.propagate(ex);
          } finally {
            generationSema.release();
          }
        });
        resultSetClosingFuture.get(1, TimeUnit.MINUTES);
      } catch (TimeoutException timeout) { // NOSONAR - not logging
        LOG.info("Batch has timed out. Adding all records received and completing batch. This may take a while");
        if (resultSetClosingFuture != null && !resultSetClosingFuture.isDone()) {
          resultSetClosingFuture.cancel(true);
          try {
            if (generationStarted.get()) {
              generationSema.acquire();
            }
          } catch (Exception ex) { // NOSONAR
            LOG.warn("Error while waiting for processing to complete", ex);
          }
        }
      } catch (Exception ex) {
        // In preview, destroy gets called after timeout which can cause a SQLException
        if (getContext().isPreview() && ex instanceof SQLException) {
          LOG.warn("Exception while previewing", ex);
          return NULL;
        }
        LOG.error("Error while attempting to produce records", ex);
        errorRecordHandler.onError(JDBC_44, Throwables.getStackTraceAsString(ex));
      }
      if (offsetReference.get() != null) {
        recordsProduced = true;
      }
    }
    if (offsetReference.get() != null) {
      String offset = offsetReference.get().toString();
      LOG.debug("Returning offset: " + offset);
      return offset;
    } else {
      if (configBean.bufferLocally) {
        String lastCommit = ZERO;
        int lastSeq = 0;
        if (!StringUtils.isEmpty(lastSourceOffset)) {
          Offset lastOffset = new Offset(lastSourceOffset);
          lastCommit = lastOffset.scn;
          lastSeq = lastOffset.sequence;
        }
        Offset newOffset = new Offset(VERSION_UNCOMMITTED, startDate, lastCommit, lastSeq);
        String offset = newOffset.toString();
        LOG.debug("Returning offset: " + offset);
        return offset;
      }
      LOG.debug("Returning empty offset");
      return lastSourceOffset == null ? "" : lastSourceOffset;
    }
  }

  private void generateRecords(
      int batchSize,
      PreparedStatement selectChanges,
      BatchMaker batchMaker,
      boolean forceNewResultSet
  ) throws SQLException, StageException, ParseException {
    String operation;
    StringBuilder query = new StringBuilder();
    ResultSet resultSet;
    int countToCheck = 0;
    if (!currentResultSet.isPresent()) {
      resultSet = selectChanges.executeQuery();
      currentResultSet = Optional.of(resultSet);
    } else {
      resultSet = currentResultSet.get();
    }

    int count = 0;
    int recordsSent = 0;
    boolean incompleteRedoStatement;
    incompleteBatch = !resultSet.next();
    try {
      while (!incompleteBatch) {
        query.append(resultSet.getString(5));
        // CSF is 1 if the query is incomplete, so read the next row before parsing
        // CSF being 0 means query is complete, generate the record
        if (resultSet.getInt(9) == 0) {
          count++;
          incompleteRedoStatement = false;
          BigDecimal scnDecimal = resultSet.getBigDecimal(1);
          String scn = scnDecimal.toPlainString();
          String username = resultSet.getString(2);
          short op = resultSet.getShort(3);
          String timestamp = resultSet.getString(4);
          LocalDateTime tsDate = Timestamp.valueOf(timestamp).toLocalDateTime();
          if (configBean.bufferLocally) {
            this.lastSCN = scnDecimal;
            this.lastSCNTimestamp = tsDate;
          }
          String table = resultSet.getString(6);
          BigDecimal commitSCN = resultSet.getBigDecimal(7);
          String queryString = query.toString();
          int seq = resultSet.getInt(8);
          String xidUsn = String.valueOf(resultSet.getLong(9));
          String xidSlt = String.valueOf(resultSet.getString(10));
          String xidSqn = String.valueOf(resultSet.getString(11));
          lastSCN = scnDecimal;
          String rsId = resultSet.getString(12);
          Object ssn = resultSet.getObject(13);
          String xid = xidUsn + "." + xidSlt + "." + xidSqn;
          TransactionIdKey key = new TransactionIdKey(xid);
          bufferedRecordsLock.lock();
          try {
            if (bufferedRecords.containsKey(key)
                && bufferedRecords.get(key).contains(new RecordSequence(null, 0, rsId, ssn))) {
              advanceResultSet(resultSet);
              continue;
            }
          } finally {
            bufferedRecordsLock.unlock();
          }
          Offset offset;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Commit SCN = " + commitSCN + ", SCN = " + scn + ", Redo SQL = " + queryString);
          }
          sqlListener.reset();
          plsqlLexer lexer = new plsqlLexer(new ANTLRInputStream(queryString));
          CommonTokenStream tokenStream = new CommonTokenStream(lexer);
          plsqlParser parser = new plsqlParser(tokenStream);
          ParserRuleContext ruleContext = null;
          int operationCode = -1;
          switch (op) {
            case UPDATE_CODE:
            case SELECT_FOR_UPDATE_CODE:
              ruleContext = parser.update_statement();
              operationCode = OperationType.UPDATE_CODE;
              break;
            case INSERT_CODE:
              ruleContext = parser.insert_statement();
              operationCode = OperationType.INSERT_CODE;
              break;
            case DELETE_CODE:
              ruleContext = parser.delete_statement();
              operationCode = OperationType.DELETE_CODE;
              break;
            case DDL_CODE:
            case COMMIT_CODE:
            case ROLLBACK_CODE:
              break;
            default:
              errorRecordHandler.onError(JDBC_43, queryString);
              continue;
          }

          if (op != DDL_CODE && op != COMMIT_CODE && op != ROLLBACK_CODE) {
            operation = OperationType.getLabelFromIntCode(operationCode);
            if (configBean.bufferLocally) {
              // Since local buffering offsets are commit scn and number of records sent, we still stick with the last
              // offset before this one.
              offset = offsetReference.get();
            } else {
              offset = new Offset(VERSION_STR, null, commitSCN.toPlainString(), seq);
            }
            // Walk it and attach our sqlListener
            parseTreeWalker.walk(sqlListener, ruleContext);
            Map<String, String> columns = sqlListener.getColumns();
            Map<String, Field> fields = new HashMap<>();
            String id = configBean.bufferLocally ?
                rsId + OFFSET_DELIM + ssn : offset.scn + OFFSET_DELIM + offset.sequence;
            Record record = getContext().createRecord(id);
            Record.Header recordHeader = record.getHeader();

            for (Map.Entry<String, String> column : columns.entrySet()) {
              String columnName = column.getKey();
              try {
                fields.put(columnName, objectToField(table, columnName, column.getValue()));
              } catch (RowIdException ignored) {
                advanceResultSet(resultSet);
                continue;
              }
              if (decimalColumns.containsKey(table) && decimalColumns.get(table).containsKey(columnName)) {
                int precision = decimalColumns.get(table).get(columnName).precision;
                int scale = decimalColumns.get(table).get(columnName).scale;
                recordHeader.setAttribute("jdbc." + columnName + ".precision", String.valueOf(precision));
                recordHeader.setAttribute("jdbc." + columnName + ".scale", String.valueOf(scale));
              }
            }
            recordHeader.setAttribute(SCN, scn);
            recordHeader.setAttribute(USER, username);
            recordHeader.setAttribute(OPERATION, operation);
            recordHeader.setAttribute(TIMESTAMP_HEADER, timestamp);
            recordHeader.setAttribute(TABLE, table);
            recordHeader.setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(operationCode));
            recordHeader.setAttribute(SEQ, String.valueOf(seq));
            recordHeader.setAttribute(XID, xid);
            record.set(Field.create(fields));
            if (LOG.isDebugEnabled()) {
              LOG.debug(Utils.format("Adding {} to batchmaker: {}", record, batchMaker.toString()));
            }
            if (record.getEscapedFieldPaths().size() > 0) {
              if (configBean.bufferLocally) {
                bufferedRecordsLock.lock();
                try {
                  HashQueue<RecordSequence> records =
                      bufferedRecords.computeIfAbsent(key, x -> {
                            x.setTxnStartTime(tsDate);
                            return new HashQueue<>();
                          }
                      );
                  int nextSeq = records.isEmpty() ? 1 : records.tail().seq + 1;
                  records.add(new RecordSequence(record, nextSeq, rsId, ssn));
                } finally {
                  bufferedRecordsLock.unlock();
                }
              } else {
                batchMaker.addRecord(record);
              }
            }
          } else if (configBean.bufferLocally && (op == COMMIT_CODE || op == ROLLBACK_CODE)) {
            // so this commit was previously processed or it is a rollback, so don't care.
            if (op == ROLLBACK_CODE || scnDecimal.compareTo(resumeCommitSCN) < 0) {
              if (scnDecimal.compareTo(resumeCommitSCN) < 0) {
                offset = new Offset(VERSION_UNCOMMITTED, startDate, resumeCommitSCN.toPlainString(), resumeSeq);
              } else {
                offset = new Offset(VERSION_UNCOMMITTED, tsDate, scn, 0);
              }
              bufferedRecordsLock.lock();
              try {
                bufferedRecords.remove(key);
              } finally {
                bufferedRecordsLock.unlock();
              }
            } else {
              boolean equal = false;
              bufferedRecordsLock.lock();
              try {
              // so this is the commit which we were processing when we died, so resume from where we left off.
              if (scnDecimal.compareTo(resumeCommitSCN) == 0 && bufferedRecords.containsKey(key)) {
                equal = true;
                Iterator<RecordSequence> txn = bufferedRecords.get(key).iterator();
                while (txn.hasNext() && txn.next().seq <= resumeSeq) {
                  txn.remove();
                }
              }
              resumeCommitSCN = new BigDecimal(scn);
              int lastSeq = 0;
              int bufferedRecordsToBeRemoved = bufferedRecords.getOrDefault(key, EMPTY_LINKED_HASHSET).size();
                if (bufferedRecordsToBeRemoved > 0) {

                  lastSeq = addRecordsToBatch(batchSize, batchMaker, xid);
                  if (equal) {
                    // TODO: Bug in this one. If equal and lastSeq returned 0, then use 0.
                    lastSeq = Math.max(resumeSeq, lastSeq);
                    if (lastSeq > resumeSeq) {
                      recordsSent += (lastSeq - resumeSeq);
                    }
                  } else {
                    recordsSent += lastSeq;
                  }
                }
                resumeSeq = lastSeq;
                offset = new Offset(VERSION_UNCOMMITTED, tsDate, scn, lastSeq);
              } finally {
                bufferedRecordsLock.unlock();
              }
            }
          } else {
            offset = new Offset(VERSION_STR, null, scn, 0);
            boolean sendSchema = false;
            // Event is sent on every DDL, but schema is not always sent.
            // Schema sending logic:
            // CREATE/ALTER: Schema is sent if the schema after the ALTER is newer than the cached schema
            // (which we would have sent as an event earlier, at the last alter)
            // DROP/TRUNCATE: Schema is not sent, since they don't change schema.
            DDL_EVENT type = getDdlType(queryString);
            if (type == DDL_EVENT.ALTER || type == DDL_EVENT.CREATE) {
              sendSchema = refreshSchema(scnDecimal, table);
            }
            getContext().toEvent(createEventRecord(type, queryString, table, offset.toString(), sendSchema));
          }
          this.offsetReference.set(offset);
          query.setLength(0);
        } else {
          incompleteRedoStatement = true;
        }
        if (!incompleteRedoStatement) {
          countToCheck = configBean.bufferLocally ? recordsSent : count;
          if (countToCheck >= batchSize) {
            LOG.info("Batch count = " + countToCheck);
            break;
          }
        }
        advanceResultSet(resultSet);
      }
    } catch (SQLException ex) {
      if (ex.getErrorCode() == MISSING_LOG_FILE) {
      } else if (ex.getErrorCode() != RESULTSET_CLOSED_AS_LOGMINER_SESSION_CLOSED) {
        LOG.warn("SQL Exception while retrieving records", ex);
      }
      if (!resultSet.isClosed()) {
        resultSet.close();
      }
      currentResultSet = Optional.empty();
    } finally {
      LOG.debug(COUNT_FOR_BATCH_AND_BATCH_SIZE, countToCheck, batchSize);
      if (forceNewResultSet || incompleteBatch) {
        closeResultSet(resultSet);
        lastSCN = null;
      }
    }
  }

  private void advanceResultSet(ResultSet resultSet) throws SQLException {
    if (resultSet!= null && !resultSet.isClosed()) {
      incompleteBatch = !resultSet.next();
    }
  }

  private long localDateTimeToEpoch(LocalDateTime date) {
    return date.atZone(ZoneId.systemDefault()).toEpochSecond();
  }

  private void closeResultSet(ResultSet resultSet) throws SQLException {
    resultSet.close();
    currentResultSet = Optional.empty();
  }

  private int addRecordsToBatch(int batchSize, BatchMaker batchMaker, String xid) {
    TransactionIdKey key = new TransactionIdKey(xid);
    bufferedRecordsLock.lock();
    try {
    HashQueue<RecordSequence> records = bufferedRecords.getOrDefault(key, EMPTY_LINKED_HASHSET);
      int lastSeq = 0; //implies nothing read.
      for (int i = 0; i < batchSize && !records.isEmpty(); i++) {
        RecordSequence r = records.remove();
        batchMaker.addRecord(r.record);
        lastSeq = r.seq;
      }
      if (records.isEmpty()) {
        currentCommitTxn = Optional.empty();
        bufferedRecords.remove(key);
      } else {
        currentCommitTxn = Optional.of(xid);
      }
      return lastSeq;
    } finally {
      bufferedRecordsLock.unlock();
    }
  }

  private EventRecord createEventRecord(
      DDL_EVENT type,
      String redoSQL,
      String table,
      String scnSeq,
      boolean sendSchema
  ) {
    EventRecord event = getContext().createEventRecord(type.name(), 1, scnSeq);
    event.getHeader().setAttribute(TABLE, table);
    if (redoSQL != null) {
      event.getHeader().setAttribute(DDL_TEXT, redoSQL);
    }
    if (sendSchema) {
      // Note that the schema inserted is the *current* schema and not the result of the DDL.
      // Getting the schema as a result of the DDL is not possible.
      // We actually don't know the schema at table creation ever, but just the schema when we started. So
      // trying to figure out the schema at the time of the DDL is not really possible since this DDL could have occured
      // before the source started. Since we allow only types to be bigger and no column drops, this is ok.
      Map<String, Integer> schema = tableSchemas.get(table);
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
  private boolean refreshSchema(BigDecimal scnDecimal, String table) throws SQLException {
    try {
      if (!tableSchemaLastUpdate.containsKey(table) || scnDecimal.compareTo(tableSchemaLastUpdate.get(table)) > 0) {
        if (containerized) {
          try (Statement switchToPdb = connection.createStatement()) {
            switchToPdb.execute("ALTER SESSION SET CONTAINER = " + configBean.pdb);
          }
        }
        tableSchemas.put(table, getTableSchema(table));
        tableSchemaLastUpdate.put(table, scnDecimal);
        return true;
      }
      return false;
    } finally {
      alterSession();
    }
  }

  private LocalDateTime refreshStartDate(BigDecimal commitSCN) throws SQLException {

    // If an incomplete batch was sent, use the ending time of the last session for starting a new one.
    refreshed = true;
    if (incompleteBatch) {
      lastRefresh = System.currentTimeMillis();
      return lastEndTime;
    }

    // Either it is an hour since the last refresh, or the last commit scn is the same as now (middle of a txn)
    // or resultset was closed due to error, don't refresh
    if ((System.currentTimeMillis() - lastRefresh <= ONE_HOUR)
        || (commitSCN.equals(scnAtLastRefresh))
        || (currentResultSet.isPresent() && !currentResultSet.get().isClosed())) {
      LOG.debug(START_DATE_REFRESH_NOT_REQUIRED);
      refreshed = false;
      return startDate;
    }

    startLogMinerUsingGivenSCNs(commitSCN, commitSCN);
    try (ResultSet rs = getCommitTimestamp.executeQuery()) {
      if (rs.next()) {
        LocalDateTime date = rs.getTimestamp(1).toLocalDateTime();
        lastRefresh = System.currentTimeMillis();
        scnAtLastRefresh = commitSCN;
        if (currentResultSet.isPresent()) {
          currentResultSet.get().close();
        }
        LOG.debug(START_DATE_REFRESHED_TO, date);
        return date;
      }
    }
    throw new IllegalStateException(Utils.format(
        "SCN: '{}' is not valid and cannot be found in LogMiner logs", commitSCN.toPlainString()));
  }

  private LocalDateTime getEndTimeForStartTime(LocalDateTime startTime) {
    LocalDateTime sessionMax = startTime.plusSeconds(configBean.logminerWindow);
    LocalDateTime now = LocalDateTime.now();
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
        LOG.debug("Started LogMiner with start SCN: {} and end SCN: {}",
            oldestSCN.toPlainString(), endSCN.toPlainString());
      }
    } catch (SQLException ex) {
      LOG.debug("SQLException while starting LogMiner", ex);
      throw ex;
    }
  }

  private void startLogMinerUsingGivenDates(String startDate, String endDate) throws SQLException {
    try {
      LOG.debug(TRYING_TO_START_LOG_MINER_WITH_START_DATE_AND_END_DATE, startDate, endDate);
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
    driverProperties.putAll(hikariConfigBean.driverProperties);
    if (connection == null) { // For tests, we set a mock connection
      try {
        dataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean, driverProperties);
        connection = dataSource.getConnection();
        connection.setAutoCommit(false);
      } catch (StageException | SQLException e) {
        LOG.error("Error while connecting to DB", e);
        issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(), CONNECTION_STR, JDBC_00, e.toString()));
        return issues;
      }
    }

    String container = configBean.pdb;
    configBean.baseConfigBean.database =
        configBean.baseConfigBean.caseSensitive ?
            configBean.baseConfigBean.database.trim() :
            configBean.baseConfigBean.database.trim().toUpperCase();
    List<String> tables;

    try {
      initializeStatements();
      alterSession();
    } catch (SQLException ex) {
      LOG.error("Error while creating statement", ex);
      issues.add(getContext().createConfigIssue(
          Groups.CDC.name(), "oracleCDCConfigBean.baseConfigBean.database", JDBC_00, configBean.baseConfigBean.database));
    }
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
          configBean.startDate = LocalDateTime.now().format(dtFormatter);
          // fall-through
        case DATE:
          try {
            LocalDateTime startDate = getDate(configBean.startDate);
            if (startDate.isAfter(LocalDateTime.now())) {
              issues.add(getContext().createConfigIssue(CDC.name(), "oracleCDCConfigBean.startDate", JDBC_48));
            }
          } catch (ParseException ex) {
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
      tables = new ArrayList<>(configBean.baseConfigBean.tables.size());
      for (String table : configBean.baseConfigBean.tables) {
        table = table.trim();
        if (!configBean.baseConfigBean.caseSensitive) {
          tables.add(table.toUpperCase());
        } else {
          tables.add(table);
        }
      }
      validateTablePresence(reusedStatement, tables, issues);
      if (!issues.isEmpty()) {
        return issues;
      }
      for (String table : tables) {
        table = table.trim();
        try {
          tableSchemas.put(table, getTableSchema(table));
          if (scn != null) {
            tableSchemaLastUpdate.put(table, scn);
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
      issues.add(getContext().createConfigIssue(
          Groups.CDC.name(), "oracleCDCConfigBean.baseConfigBean.database",
          JDBC_00, configBean.baseConfigBean.database));
      return issues;
    }

    final String ddlTracking = shouldTrackDDL ? " + DBMS_LOGMNR.DDL_DICT_TRACKING" : "";

    final String readCommitted = configBean.bufferLocally ? "" : "+ DBMS_LOGMNR.COMMITTED_DATA_ONLY";

    this.logMinerProcedure = "BEGIN"
        + " DBMS_LOGMNR.START_LOGMNR("
        + " {},"
        + " {},"
        + " OPTIONS => DBMS_LOGMNR." + configBean.dictionary.name()
        + "          + DBMS_LOGMNR.CONTINUOUS_MINE"
        + readCommitted
        + "          + DBMS_LOGMNR.NO_ROWID_IN_STMT"
        + "          + DBMS_LOGMNR.NO_SQL_DELIMITER"
        + ddlTracking
        + ");"
        + " END;";

    final String base =
        "SELECT SCN, USERNAME, OPERATION_CODE, TIMESTAMP, SQL_REDO, TABLE_NAME, " + commitScnField +
            ", SEQUENCE#, CSF, XIDUSN, XIDSLT, XIDSQN, RS_ID, SSN" +
            " FROM V$LOGMNR_CONTENTS" +
            " WHERE ";

    final String tableCondition = Utils.format("SEG_OWNER='{}' AND TABLE_NAME IN ({})",
        configBean.baseConfigBean.database, formatTableList(tables));

    final String commitRollbackCondition = Utils.format("OPERATION_CODE = {} OR OPERATION_CODE = {}",
        COMMIT_CODE, ROLLBACK_CODE);

    final String operationsCondition = "OPERATION_CODE IN (" + getSupportedOperations() + ")";

    final String restartNonBufferCondition = Utils.format("((" + commitScnField + " = ? AND SEQUENCE# > ?) OR "
        + commitScnField + "  > ?)" + (shouldTrackDDL ? " OR (OPERATION_CODE = {} AND SCN > ?)" : ""), DDL_CODE);

    if (configBean.bufferLocally) {
      resumeString = startString = base + " (" +
          "(" + tableCondition + " AND (" + operationsCondition + "))" +
          " OR (" + commitRollbackCondition + ")" +
          ")";
    } else {
      startString = base +
          " (" + tableCondition + " AND (" + operationsCondition + "))";
      resumeString = startString + "AND (" + restartNonBufferCondition + ")";
    }

    // For resume, none of these conditions matter. We will use the commit scn from offset to start looking for
    // committed data.
    switch (configBean.startValue) {
      case SCN:
        startString = startString + " AND SCN >= " + configBean.startSCN;
        resumeCommitSCN = new BigDecimal(configBean.startSCN);
        break;
      case LATEST:
        configBean.startDate = LocalDateTime.now().format(dtFormatter);
        // fall through
      case DATE:
        startString = startString + " AND TIMESTAMP >= TO_DATE('" + configBean.startDate + "', 'DD-MM-YYYY HH24:MI:SS')";
        resumeCommitSCN = BigDecimal.ZERO;
        break;
      default:
        throw new IllegalStateException("Unknown start value!");
    }

    try {
      initializeLogMnrStatements();
    } catch (SQLException ex) {
      LOG.error("Error while creating statement", ex);
      issues.add(getContext().createConfigIssue(
          Groups.CDC.name(), "oracleCDCConfigBean.baseConfigBean.database", JDBC_00, configBean.baseConfigBean.database));
    }

    if (configBean.baseConfigBean.caseSensitive) {
      sqlListener.setCaseSensitive();
    }

    if (configBean.txnWindow >= configBean.logminerWindow) {
      issues.add(getContext().createConfigIssue(Groups.CDC.name(), "oracleCDCConfigBean.logminerWindow", JDBC_81));
    }

    if (configBean.bufferLocally) {
      discardExecutor.schedule(this::discardOldUncommitted, configBean.txnWindow, TimeUnit.SECONDS);
    }

    return issues;
  }

  private void initializeStatements() throws SQLException {
    getOldestSCN = connection.prepareStatement(GET_OLDEST_SCN);
    getLatestSCN = connection.prepareStatement(CURRENT_SCN);
    dateStatement = connection.prepareStatement(NLS_DATE_FORMAT);
    tsStatement = connection.prepareStatement(NLS_TIMESTAMP_FORMAT);
    numericFormat = connection.prepareStatement(NLS_NUMERIC_FORMAT);
    switchContainer = connection.prepareStatement(SWITCH_TO_CDB_ROOT);
  }

  private void initializeLogMnrStatements() throws SQLException {
    resume = getSelectChangesStatement();
    start = connection.prepareStatement(startString);
    startLogMnrForCommitSCN = connection.prepareCall(
        Utils.format(logMinerProcedure, "STARTSCN => ?", "ENDSCN => ?"));
    startLogMnrForData = connection.prepareCall(
        Utils.format(logMinerProcedure, "STARTTIME => ?", "ENDTIME => ?"));
    startLogMnrForResume = connection.prepareCall(
        Utils.format(logMinerProcedure, "STARTSCN => ?", "ENDTIME => ?"));
    endLogMnr = connection.prepareCall("BEGIN DBMS_LOGMNR.END_LOGMNR; END;");
    getCommitTimestamp = connection.prepareStatement(GET_COMMIT_TS);
    LOG.debug("Redo select query for start = " + startString);
    LOG.debug("Redo select query for resume = " + resumeString);
  }

  private PreparedStatement getSelectChangesStatement() throws SQLException {
    return resume = connection.prepareStatement(resumeString);
  }

  private String formatTableList(List<String> tables) {
    List<String> quoted = new ArrayList<>(tables.size());
    for (String table : tables) {
      quoted.add("'" + table + "'");
    }
    Joiner joiner = Joiner.on(',');
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
      LOG.debug("Current latest SCN is: " + scn.toPlainString());
      return scn;
    }
  }

  private void validateTablePresence(Statement statement, List<String> tables, List<ConfigIssue> issues) {
    for (String table : tables) {
      try {
        statement.execute("SELECT * FROM \"" + configBean.baseConfigBean.database + "\".\"" + table + "\" WHERE 1 = 0");
      } catch (SQLException ex) {
        StringBuilder sb = new StringBuilder("Table: ").append(table).append(" does not exist.");
        if (StringUtils.isEmpty(configBean.pdb)) {
          sb.append(" PDB was not specified. If the database was created inside a PDB, please specify PDB");
        }
        LOG.error(sb.toString(), ex);
        issues.add(getContext().createConfigIssue(Groups.CDC.name(), "oracleCDCConfigBean.baseConfigBean.tables", JDBC_16, table));
      }
    }
  }

  private Map<String, Integer> getTableSchema(String tableName) throws SQLException {
    Map<String, Integer> columns = new HashMap<>();
    String query = "SELECT * FROM \"" + configBean.baseConfigBean.database + "\".\"" + tableName + "\" WHERE 1 = 0";
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
          dateTimeColumns.computeIfAbsent(tableName, k -> new HashMap<>());
          dateTimeColumns.get(tableName).put(colName, md.getColumnTypeName(i));
        }

        if (colType == Types.DECIMAL || colType == Types.NUMERIC) {
          decimalColumns.computeIfAbsent(tableName, k -> new HashMap<>());
          decimalColumns.get(tableName).put(colName, new PrecisionAndScale(md.getPrecision(i), md.getScale(i)));
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

    try {
      if (endLogMnr != null && !endLogMnr.isClosed())
        endLogMnr.execute();
    } catch (SQLException ex) {
      LOG.warn("Error while stopping LogMiner", ex);
    }

    // Close all statements
    closeStatements(dateStatement, startLogMnrForCommitSCN, startLogMnrForData, getLatestSCN, getOldestSCN, endLogMnr);

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

    if (resultSetClosingFuture != null && !resultSetClosingFuture.isDone()) {
      resultSetClosingFuture.cancel(true);
    }
    resultSetExecutor.shutdown();
    discardExecutor.shutdown();
  }

  private void closeStatements(Statement... statements) {
    if (statements == null) {
      return;
    }

    for (Statement stmt : statements) {
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
        LOG.warn("Error while closing connection to database", e);
      }
    }
  }

  private Field objectToField(String table, String column, String columnValue) throws ParseException, StageException {
    Map<String, Integer> tableSchema = tableSchemas.get(table);
    if (!tableSchema.containsKey(column)) {
      if (column.equals(ROWID)) { // ROWID in caps is the reserved keyword.
        throw new RowIdException();
      }
      throw new StageException(JDBC_54, column, table);
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
        field = Field.create(Field.Type.BYTE_ARRAY, columnValue);
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
        String actualType = dateTimeColumns.get(table).get(column);
        field = getDateTimeStampField(column, columnValue, columnType, actualType);
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
        //case Types.REF_CURSOR: // JDK8 only
      case Types.SQLXML:
      case Types.STRUCT:
        //case Types.TIME_WITH_TIMEZONE: // JDK8 only
        //case Types.TIMESTAMP_WITH_TIMEZONE: // JDK8 only
      default:
        throw new StageException(JDBC_37, columnType, column);
    }

    return field;
  }

  /**
   * This method returns an {@linkplain Field} that represents a DATE, TIME or TIMESTAMP. It is possible for user to upgrade
   * a field from DATE to TIMESTAMP, and if we read the table schema on startup after this upgrade, we would assume the field
   * should be returned as DATETIME field. But it is possible that the first change we read was made before the upgrade from
   * DATE to TIMESTAMP. So we check whether the returned SQL has TO_TIMESTAMP - if it does we return it as DATETIME, else we
   * return it as DATE.
   */
  private Field getDateTimeStampField(String column, String columnValue, int columnType, String actualType) throws StageException, ParseException {
    Field.Type type;
    if (DATE.equalsIgnoreCase(actualType)) {
      type = Field.Type.DATE;
    } else if (TIME.equalsIgnoreCase(actualType)) {
      type = Field.Type.TIME;
    } else if (TIMESTAMP.equalsIgnoreCase(actualType)) {
      type = Field.Type.DATETIME;
    } else {
      throw new StageException(JDBC_37, columnType, column);
    }
    if (columnValue == null) {
      return Field.create(type, null);
    } else {
      Optional<String> ts = matchDateTimeString(toTimestampPattern.matcher(columnValue));
      if (ts.isPresent()) {
        return Field.create(type, Timestamp.valueOf(ts.get()));
      }
      // We did not find TO_TIMESTAMP, so try TO_DATE
      Optional<String> dt = matchDateTimeString(toDatePattern.matcher(columnValue));
      return Field.create(Field.Type.DATE, dt.isPresent() ?
          Date.from(getDate(dt.get()).atZone(ZoneId.systemDefault()).toInstant()) : null);
    }
  }

  private static Optional<String> matchDateTimeString(Matcher m) {
    if (!m.find()) {
      return Optional.empty();
    }
    return Optional.of(m.group(1));
  }

  private LocalDateTime getDate(String s) throws ParseException {
    return LocalDateTime.parse(s, dtFormatter);
  }

  private void alterSession() throws SQLException {
    if (containerized) {
      switchContainer.execute();
    }
    dateStatement.execute();
    tsStatement.execute();
    numericFormat.execute();
  }

  private void discardOldUncommitted() {
    bufferedRecordsLock.lock();
    try {
      AtomicInteger txnDiscarded = new AtomicInteger(0);
      AtomicInteger recordsDiscarded = new AtomicInteger(0);
      LOG.info("Removing expired transactions.");
      Iterator<Map.Entry<TransactionIdKey, HashQueue<RecordSequence>>> iter = bufferedRecords.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<TransactionIdKey, HashQueue<RecordSequence>> entry = iter.next();
        if (expired(entry)) {
          LOG.info("Removing transaction with id: " + entry.getKey().txnId);
          if (!configBean.discardExpired) {
            entry.getValue().forEach(x -> {
                  getContext().toError(x.record, JDBC_84, entry.getKey().txnId, entry.getKey().txnStartTime);
                  recordsDiscarded.incrementAndGet();
                }
            );
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
  private boolean expired(Map.Entry<TransactionIdKey, HashQueue<RecordSequence>> entry) {
    return startDate != null && // Can be null if starting from SCN and first batch is not complete yet.
        entry.getKey().txnStartTime.isBefore(startDate.minusSeconds(configBean.txnWindow)) &&
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

  private class PrecisionAndScale {
    int precision;
    int scale;

    PrecisionAndScale(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }
  }

  private class RecordSequence {
    final Record record;
    final int seq;
    final String rsId;
    final Object ssn;

    RecordSequence(Record record, int seq, String rsId, Object ssn) {
      this.record = record;
      this.seq = seq;
      this.rsId = rsId;
      this.ssn = ssn;
    }

    @Override
    public boolean equals(Object o) {
      return o != null
          && o instanceof RecordSequence
          && this.rsId.equals(((RecordSequence) o).rsId)
          && this.ssn.equals(((RecordSequence) o).ssn);
    }

    @Override
    public int hashCode() {
      return rsId.hashCode() + ssn.hashCode();
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
      return o!= null
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
    final LocalDateTime timestamp;
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
          version.equals("v3") ?
              LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(splits[index++])), ZoneId.systemDefault()): null;
      this.scn = splits[index++];
      this.sequence = Integer.parseInt(splits[index]);
    }

    public String toString() {
      return version + OFFSET_DELIM +
          (timestamp != null ? localDateTimeToEpoch(timestamp) + OFFSET_DELIM : "") +
          scn + OFFSET_DELIM +
          sequence;
    }
  }

  private class RowIdException extends RuntimeException {

  }

}
