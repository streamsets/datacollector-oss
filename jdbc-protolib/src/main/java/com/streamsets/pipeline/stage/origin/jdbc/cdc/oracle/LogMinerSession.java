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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.ChangeTypeValues;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.COMMIT_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.DDL_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.DELETE_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.INSERT_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.ROLLBACK_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.SELECT_FOR_UPDATE_CODE;
import static com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode.UPDATE_CODE;


/**
 * Handles LogMiner sessions for an Oracle database connection.
 */
public class LogMinerSession {
  private static final Logger LOG = LoggerFactory.getLogger(LogMinerSession.class);

  // Option flags for LogMiner start command
  private static final String CONTINUOUS_MINE_OPTION = "DBMS_LOGMNR.CONTINUOUS_MINE";
  private static final String NO_SQL_DELIMITER_OPTION = "DBMS_LOGMNR.NO_SQL_DELIMITER";
  private static final String COMMITTED_DATA_ONLY_OPTION = "DBMS_LOGMNR.COMMITTED_DATA_ONLY";
  private static final String DDL_DICT_TRACKING_OPTION = "DBMS_LOGMNR.DDL_DICT_TRACKING";
  private static final String DICT_FROM_REDO_LOGS_OPTION = "DBMS_LOGMNR.DICT_FROM_REDO_LOGS";
  private static final String DICT_FROM_ONLINE_CATALOG_OPTION = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG";

  // Templates to build LogMiner start/stop commands
  private static final String STOP_LOGMNR_CMD = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;";
  private static final String START_LOGMNR_CMD = "BEGIN"
      + " DBMS_LOGMNR.START_LOGMNR("
      + "   {}" // Placeholder for START_TIME_ARG or START_SCN_ARG
      + "   {}" // Placeholder for END_TIME_ARG or END_SCN_ARG
      + "   OPTIONS => {}" // Option flags
      + " );"
      + " END;";
  private static final String START_TIME_ARG = "STARTTIME => TO_DATE('{}', 'DD-MM-YYYY HH24:MI:SS'),";
  private static final String END_TIME_ARG = "ENDTIME => TO_DATE('{}', 'DD-MM-YYYY HH24:MI:SS'),";
  private static final String START_SCN_ARG = "STARTSCN => {},";
  private static final String END_SCN_ARG = "ENDSCN => {},";
  private static final String DATETIME_FORMAT = "dd-MM-yyyy HH:mm:ss";  // Must match START_TIME_ARG and END_TIME_ARG.
  private static final String TO_DATE_FORMAT = "DD-MM-YYYY HH24:MI:SS";

  // Template to query LogMiner content after a session was started. Placeholders for the COMMIT_SCN column and the
  // where conditions.
  private static final String SELECT_LOGMNR_CONTENT_QUERY =
      "SELECT SCN, USERNAME, OPERATION_CODE, TIMESTAMP, SQL_REDO, TABLE_NAME, {}, SEQUENCE#, CSF,"
      + " XIDUSN, XIDSLT, XIDSQN, RS_ID, SSN, SEG_OWNER, ROLLBACK, ROW_ID, REDO_VALUE, UNDO_VALUE "
      + " FROM V$LOGMNR_CONTENTS "
      + " WHERE {}";
  private static final String COMMIT_SCN_COLUMN = "COMMIT_SCN";
  private static final String COMMIT_SCN_COLUMN_OLD = "CSCN";  // Oracle version < 11

  // Templates to manually manage the LogMiner redo log list.
  private static final String ADD_LOGFILE_NEWLIST_CMD = "BEGIN DBMS_LOGMNR.ADD_LOGFILE(?, DBMS_LOGMNR.NEW); END;";
  private static final String ADD_LOGFILE_APPEND_CMD = "BEGIN DBMS_LOGMNR.ADD_LOGFILE(?, DBMS_LOGMNR.ADDFILE); END;";
  private static final String REMOVE_LOGFILE_CMD = "BEGIN DBMS_LOGMNR.REMOVE_LOGFILE(?); END;";

  // Query to check if the database is treating '' (empty string) as null.
  // This is necessary because, although Oracle treats the empty string as null, it warns that this might
  // not happen in future versions of Oracle. We will interpret the empty string as null only when this query
  // gives a positive result.
  public static final String CHECK_EMPTY_STRING_EQUALS_NULL_QUERY = "select 1 from dual where '' is null";

  // Template to retrieve the list of redo log files given a time range [first, next) of interest.
  // Note the default NEXT_TIME and NEXT_CHANGE# values for the CURRENT online logs (null and <max SCN> respectively)
  // are replaced by the LOCALTIMESTAMP and CURRENT_SCN. This will enable an easier redo log and mining window
  // handling.
  private static final String SELECT_REDO_LOGS_QUERY =
      " SELECT NAME, NULL GROUP#, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, "
      + "    DICTIONARY_BEGIN, DICTIONARY_END, STATUS, 'NO' AS ONLINE_LOG, 'YES' AS ARCHIVED "
      + " FROM V$ARCHIVED_LOG "
      + " WHERE STATUS = 'A' AND STANDBY_DEST = 'NO' AND RESETLOGS_ID = :resetLogsId AND "
      + "     ((FIRST_TIME <= :first AND NEXT_TIME >= :first) OR "
      + "      (FIRST_TIME < :next AND NEXT_TIME >= :next) OR "
      + "      (FIRST_TIME > :first AND NEXT_TIME < :next)) "
      + " UNION "
      + " SELECT C.MEMBER, A.GROUP#, A.THREAD#, A.SEQUENCE#, A.FIRST_TIME, "
      + "     (CASE WHEN A.STATUS = 'CURRENT' THEN LOCALTIMESTAMP ELSE A.NEXT_TIME END) NEXT_TIME, "
      + "     A.FIRST_CHANGE#, (CASE WHEN A.STATUS = 'CURRENT' THEN B.CURRENT_SCN ELSE A.NEXT_CHANGE# END) NEXT_CHANGE#, "
      + "     'NO' AS DICTIONARY_BEGIN, 'NO' AS DICTIONARY_END, A.STATUS, 'YES' AS ONLINE_LOG, A.ARCHIVED "
      + " FROM V$LOG A, V$DATABASE B, "
      + "     (SELECT GROUP#, MEMBER, ROW_NUMBER() OVER (PARTITION BY GROUP# ORDER BY GROUP#) AS ROWNO "
      + "      FROM V$LOGFILE WHERE TYPE = 'ONLINE') C "
      + " WHERE A.GROUP# = C.GROUP# AND A.MEMBERS = C.ROWNO";
  private static final String SELECT_REDO_LOGS_ARG_FIRST = ":first";
  private static final String SELECT_REDO_LOGS_ARG_NEXT = ":next";
  private static final String SELECT_REDO_LOGS_ARG_RESETLOGS = ":resetLogsId";

  // Template to retrieve the list of redo log files whose SCN range (first, next) covers a given SCN of interest.
  // Note the default NEXT_TIME and NEXT_CHANGE# values for the CURRENT online logs (null and <max SCN> respectively)
  // are replaced by the LOCALTIMESTAMP and CURRENT_SCN. This will enable an easier redo log and mining window
  // handling.
  private static final String SELECT_REDO_LOGS_FOR_SCN_QUERY =
      " SELECT NAME, NULL GROUP#, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, "
      + "    DICTIONARY_BEGIN, DICTIONARY_END, STATUS, 'NO' AS ONLINE_LOG, 'YES' AS ARCHIVED "
      + " FROM V$ARCHIVED_LOG "
      + " WHERE STATUS = 'A' AND STANDBY_DEST = 'NO' AND RESETLOGS_ID = :resetLogsId AND "
      + "     FIRST_CHANGE# <= :scn AND NEXT_CHANGE# >= :scn "
      + " UNION "
      + " SELECT C.MEMBER, A.GROUP#, A.THREAD#, A.SEQUENCE#, A.FIRST_TIME, "
      + "     (CASE WHEN A.STATUS = 'CURRENT' THEN LOCALTIMESTAMP ELSE A.NEXT_TIME END) NEXT_TIME, "
      + "     A.FIRST_CHANGE#, (CASE WHEN A.STATUS = 'CURRENT' THEN B.CURRENT_SCN ELSE A.NEXT_CHANGE# END) NEXT_CHANGE#, "
      + "     'NO' AS DICTIONARY_BEGIN, 'NO' AS DICTIONARY_END, A.STATUS, 'YES' AS ONLINE_LOG, A.ARCHIVED "
      + " FROM V$LOG A, V$DATABASE B, "
      + "     (SELECT GROUP#, MEMBER, ROW_NUMBER() OVER (PARTITION BY GROUP# ORDER BY GROUP#) AS ROWNO "
      + "      FROM V$LOGFILE WHERE TYPE = 'ONLINE') C "
      + " WHERE A.GROUP# = C.GROUP# AND A.MEMBERS = C.ROWNO";
  private static final String SELECT_REDO_LOGS_FOR_SCN_ARG = ":scn";
  private static final String SELECT_REDO_LOGS_FOR_SCN_RESETLOGS_ARG = ":resetLogsId";

  // Templates to retrieve the redo logs containing the LogMiner dictionary valid for transactions with SCN >= :scn.
  private static final String SELECT_DICTIONARY_END_QUERY =
      " SELECT NAME, NULL GROUP#, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, "
      + "    DICTIONARY_BEGIN, DICTIONARY_END, STATUS, 'NO' AS ONLINE_LOG, 'YES' AS ARCHIVED "
      + " FROM V$ARCHIVED_LOG "
      + " WHERE DICTIONARY_END = 'YES' AND STATUS = 'A' AND STANDBY_DEST = 'NO' AND RESETLOGS_ID = :resetLogsId AND "
      + "       FIRST_CHANGE# = (SELECT MAX(FIRST_CHANGE#) FROM V$ARCHIVED_LOG WHERE STATUS = 'A' AND "
      + "                        STANDBY_DEST = 'NO' AND RESETLOGS_ID = :resetLogsId AND DICTIONARY_END = 'YES' AND "
      + "                        FIRST_TIME <= :time) ";
  private static final String SELECT_DICTIONARY_LOGS_QUERY =
      " SELECT NAME, NULL GROUP#, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, "
      + "    DICTIONARY_BEGIN, DICTIONARY_END, STATUS, 'NO' AS ONLINE_LOG, 'YES' AS ARCHIVED "
      + " FROM V$ARCHIVED_LOG "
      + " WHERE FIRST_CHANGE# >= (SELECT MAX(FIRST_CHANGE#) FROM V$ARCHIVED_LOG WHERE STATUS = 'A' AND "
      + "                         STANDBY_DEST = 'NO' AND RESETLOGS_ID = :resetLogsId AND DICTIONARY_BEGIN = 'YES' AND "
      + "                         FIRST_CHANGE# <= :scn AND THREAD# = :thread) AND "
      + "    FIRST_CHANGE# <= :scn AND STATUS = 'A' AND STANDBY_DEST = 'NO' AND RESETLOGS_ID = :resetLogsId AND "
      + "    THREAD# = :thread "
      + " ORDER BY FIRST_CHANGE# ";
  private static final String SELECT_DICTIONARY_TIME_ARG = ":time";
  private static final String SELECT_DICTIONARY_SCN_ARG = ":scn";
  private static final String SELECT_DICTIONARY_THREAD_ARG = ":thread";
  private static final String SELECT_DICTIONARY_RESETLOGS_ARG = ":resetLogsId";

  // Template to retrieve the list of online redo log files.
  // Note the default NEXT_TIME and NEXT_CHANGE# values for the CURRENT online logs (null and <max SCN> respectively)
  // are replaced by the LOCALTIMESTAMP and CURRENT_SCN. This will enable an easier redo log and mining window
  // handling.
  private static final String SELECT_ONLINE_REDO_LOGS_QUERY =
      " SELECT C.MEMBER, A.GROUP#, A.THREAD#, A.SEQUENCE#, A.FIRST_TIME, "
      + "     (CASE WHEN A.STATUS = 'CURRENT' THEN LOCALTIMESTAMP ELSE A.NEXT_TIME END) NEXT_TIME, "
      + "     A.FIRST_CHANGE#, (CASE WHEN A.STATUS = 'CURRENT' THEN B.CURRENT_SCN ELSE A.NEXT_CHANGE# END) NEXT_CHANGE#, "
      + "     'NO' AS DICTIONARY_BEGIN, 'NO' AS DICTIONARY_END, A.STATUS, 'YES' AS ONLINE_LOG, A.ARCHIVED "
      + " FROM V$LOG A, V$DATABASE B, "
      + "     (SELECT GROUP#, MEMBER, ROW_NUMBER() OVER (PARTITION BY GROUP# ORDER BY GROUP#) AS ROWNO "
      + "      FROM V$LOGFILE WHERE TYPE = 'ONLINE') C "
      + " WHERE A.GROUP# = C.GROUP# AND A.MEMBERS = C.ROWNO";

  // Query to retrieve the resetlogs id for the current database incarnation. This is used to filter the V$ARCHIVED_LOG
  // view and take only redo logs from the current incarnation.
  @VisibleForTesting
  static final String SELECT_CURRENT_DATABASE_INCARNATION_QUERY =
      "SELECT RESETLOGS_ID FROM V$DATABASE_INCARNATION WHERE STATUS = 'CURRENT'";

  // Templates for the getLocalDateTimeForSCN utility function.
  private static final String SELECT_TIMESTAMP_FOR_SCN =
      "SELECT TIMESTAMP FROM V$LOGMNR_CONTENTS WHERE SCN >= ? ORDER BY SCN";
  private static final String TIMESTAMP_COLUMN = "TIMESTAMP";

  // Query retrieving the current redo log files registered in the current LogMiner session.
  private static final String SELECT_LOGMNR_LOGS_QUERY = "SELECT FILENAME FROM V$LOGMNR_LOGS ORDER BY LOW_SCN";

  // Patterns to produce messages for JDBC_52 errors
  private static final String JDBC_52_LONG_PATTERN ="Action: %s - Message: %s - SQL State: %s - Vendor Code: %s";

  @VisibleForTesting
  static final String LOG_STATUS_CURRENT = "CURRENT";
  static final String RESETLOGS_STATUS_CURRENT = "CURRENT";

  private final Connection connection;
  private final int databaseVersion;
  private final String configOptions;
  private final boolean continuousMine;
  private final boolean dictionaryFromRedoLogs;
  private final boolean ddlDictTracking;
  private final boolean commitedDataOnly;
  private final DateTimeFormatter dateTimeFormatter;
  private final List<RedoLog> currentLogList;
  private final PreparedStatement queryContentStatement;
  private final BigDecimal resetLogsId;
  private boolean activeSession;
  private LocalDateTime startTime;
  private LocalDateTime endTime;
  private boolean emptyStringEqualsNull;

  /**
   * LogMinerSession builder. It allows configuring optional parameters for the LogMiner session. Actual LogMiner
   * session is eventually created through the {@link LogMinerSession#start} functions.
   */
  public static class Builder {
    private final Connection connection;
    private DictionaryValues dictionarySource = DictionaryValues.DICT_FROM_ONLINE_CATALOG;
    private boolean continuousMine;
    private boolean committedDataOnly;
    private boolean ddlDictTracking;
    private int databaseVersion;
    private List<SchemaTableConfigBean> tablesForMining;
    private List<ChangeTypeValues> trackedOperations;

    public Builder(Connection connection, int databaseVersion) {
      this.connection = Preconditions.checkNotNull(connection);
      this.databaseVersion = databaseVersion;
    }

    public Builder setContinuousMine(boolean continuousMine) {
      this.continuousMine = continuousMine;
      return this;
    }

    public Builder setDictionarySource(DictionaryValues dictionarySource) {
      this.dictionarySource = Preconditions.checkNotNull(dictionarySource);
      return this;
    }

    public Builder setCommittedDataOnly(boolean committedDataOnly) {
      this.committedDataOnly = committedDataOnly;
      return this;
    }

    public Builder setDDLDictTracking(boolean ddlDictTracking) {
      this.ddlDictTracking = ddlDictTracking;
      return this;
    }

    public Builder setTablesForMining(List<SchemaTableConfigBean> tables) {
      tablesForMining = tables;
      return this;
    }

    public Builder setTrackedOperations(List<ChangeTypeValues> operations) {
      trackedOperations = operations;
      return this;
    }

    public LogMinerSession build() throws SQLException {
      List<String> configOptions = new ArrayList<>();
      configOptions.add(NO_SQL_DELIMITER_OPTION);
      configOptions.add("DBMS_LOGMNR." + dictionarySource.name());

      if (tablesForMining == null || tablesForMining.size() == 0) {
        throw new IllegalArgumentException("At least a table must be configured for mining");
      }
      if (trackedOperations == null || trackedOperations.size() == 0) {
        throw new IllegalArgumentException("At least a database operation to be tracked must be configured");
      }
      if (continuousMine) {
        if (databaseVersion >= 19) {
          throw new IllegalArgumentException("CONTINUOUS_MINE option not supported from Oracle 19 onwards");
        }
        configOptions.add(CONTINUOUS_MINE_OPTION);
      }
      if (committedDataOnly) {
        configOptions.add(COMMITTED_DATA_ONLY_OPTION);
      }
      if (ddlDictTracking) {
        if (dictionarySource != DictionaryValues.DICT_FROM_REDO_LOGS) {
          throw new IllegalArgumentException("DDL_DICT_TRACKING option only allowed for DICT_FROM_REDO_LOGS");
        }
        configOptions.add(DDL_DICT_TRACKING_OPTION);
      }

      LogMinerSession logMinerSession = new LogMinerSession(connection, databaseVersion, createQueryContentStatement(), configOptions);
      logMinerSession.decideEmptyStringEqualsNull();
      return logMinerSession;
    }

    /**
     * Creates a PreparedStatement to query V$LOGMNR_CONTENT view. The where conditions are built according to the
     * configuration passed through the Builder.
     */
    private PreparedStatement createQueryContentStatement() throws SQLException {
      String tablesCondition = buildTablesCondition(tablesForMining);
      String opsCondition = buildOperationsCondition(trackedOperations);
      String commitScnColumn = databaseVersion >= 11 ? COMMIT_SCN_COLUMN : COMMIT_SCN_COLUMN_OLD;
      PreparedStatement result;

      if (committedDataOnly) {
        String restartCondition = Utils.format(
            "(({} = ? AND SEQUENCE# > ?) OR {} > ?) {}",
            commitScnColumn,
            commitScnColumn,
            ddlDictTracking ? Utils.format(" OR (OPERATION_CODE = {} AND SCN > ?)", DDL_CODE) : ""
        );
        String conditions = Utils.format("TIMESTAMP >= ? AND TIMESTAMP < ? AND {} AND {} AND ({})",
            tablesCondition, opsCondition, restartCondition);

        result = connection.prepareStatement(Utils.format(SELECT_LOGMNR_CONTENT_QUERY, commitScnColumn, conditions));

      } else {
        String tnxOps = Utils.format("OPERATION_CODE IN ({},{})", COMMIT_CODE, ROLLBACK_CODE);
        String conditions = Utils.format("TIMESTAMP >= ? AND TIMESTAMP < ? AND (({} AND {}) OR {})",
            tablesCondition, opsCondition, tnxOps);

        result = connection.prepareStatement(Utils.format(SELECT_LOGMNR_CONTENT_QUERY, commitScnColumn, conditions));
      }

      return result;
    }

    @VisibleForTesting
    String buildTablesCondition(List<SchemaTableConfigBean> schemaAndTableConfigs) {
      final int maxInClauseElements = 1000;  // Oracle limit for the number of elements in a IN clause.

      Multimap<String, String> schemaTablesMap = ArrayListMultimap.create();
      for (SchemaTableConfigBean config : schemaAndTableConfigs) {
        schemaTablesMap.put(config.schema, config.table);
      }

      StringJoiner schemaTableConditions = new StringJoiner(" OR ", "(", ")");

      for (String schema : schemaTablesMap.keySet()) {
        StringJoiner tableConditions = new StringJoiner(" OR ");
        List<String> tableNames = new ArrayList<>();

        for (String table: schemaTablesMap.get(schema)) {
          if (isSqlLikePattern(table)) {
            tableConditions.add(String.format("TABLE_NAME LIKE '%s'", table));
          } else {
            tableNames.add(String.format("'%s'", table));
            if (tableNames.size() == maxInClauseElements) {
              tableConditions.add(String.format("TABLE_NAME IN (%s)", String.join(",", tableNames)));
              tableNames.clear();
            }
          }
        }

        if (!tableNames.isEmpty()) {
          tableConditions.add(String.format("TABLE_NAME IN (%s)", String.join(",", tableNames)));
        }

        if (isSqlLikePattern(schema)) {
          schemaTableConditions.add(String.format("(SEG_OWNER LIKE '%s' AND (%s))", schema, tableConditions));
        } else {
          schemaTableConditions.add(String.format("(SEG_OWNER = '%s' AND (%s))", schema, tableConditions));
        }
      }

      return schemaTableConditions.toString();
    }

    private String buildOperationsCondition(List<ChangeTypeValues> operations) {
      List<Integer> supportedOps = new ArrayList<>();

      for (ChangeTypeValues change : operations) {
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
      if (ddlDictTracking) {
        supportedOps.add(DDL_CODE);
      }

      return Utils.format("OPERATION_CODE IN ({})", Joiner.on(',').join(supportedOps));
    }

    /**
     * Returns if the given string is a SQL LIKE pattern: '%' indicates any substring of zero or more characters,
     * and '_' means any single character.
     *
     * @param s The string to test.
     * @return True if <tt>s</tt> is a pattern, false otherwise.
     */
    private boolean isSqlLikePattern(String s) {
      return s.contains("%") || s.contains("_");
    }
  }

  private LogMinerSession(Connection connection, int databaseVersion, PreparedStatement queryContentStatement,
      List<String> configOptions) {
    this.connection = connection;
    this.databaseVersion = databaseVersion;
    this.queryContentStatement = queryContentStatement;
    this.configOptions = String.join(" + ", configOptions);
    continuousMine = configOptions.contains(CONTINUOUS_MINE_OPTION);
    dictionaryFromRedoLogs = configOptions.contains(DICT_FROM_REDO_LOGS_OPTION);
    ddlDictTracking = configOptions.contains(DDL_DICT_TRACKING_OPTION);
    commitedDataOnly = configOptions.contains(COMMITTED_DATA_ONLY_OPTION);
    dateTimeFormatter = new DateTimeFormatterBuilder()
        .parseLenient()
        .appendPattern(DATETIME_FORMAT)
        .toFormatter();
    currentLogList = new ArrayList<>();
    resetLogsId = queryCurrentResetLogsId();
  }

  /**
   * Starts a new LogMiner session. This closes the currently active session, if any.
   *
   * The {@code start} and {@code end} parameters define the range of redo records accessible in this session for
   * mining. When CONTINUOUS_MINE is not active and DICT_FROM_REDO_LOGS is enabled, the range is internally enlarge to
   * match the FIRST_TIME and NEXT_TIME of the oldest and most recent redo logs containing relevant data, respectively.
   *
   * @param start The initial point where LogMiner will start to mine at. If CONTINUOUS_MINE is not active and
   *    DICT_FROM_REDO_LOGS is enabled, the function shifts backwards the {@code start} to the FIRST_TIME of the
   *    oldest redo log with relevant data. Available redo records will have a timestamp later than or equal to (the
   *    adjusted) {@code start}.
   * @param end The last point where LogMiner will stop to mine at. If CONTINUOUS_MINE is not active and
   *    DICT_FROM_REDO_LOGS is enabled, the function shifts forwards the {@code end} to the NEXT_TIME of the most
   *    recent redo log with relevant data. Available redo records will have a datetime earlier than (the adjusted)
   *    {@code end}.
   * @return True if the new LogMiner session has successfully started. False when CONTINUOUS_MINE is disabled and
   *    required redo logs are in a transient state (i.e. current online log is required but log rotation is in
   *    progress, or archiving process for a relevant log is in progress).
   * @throws StageException If an invalid range was configured, no CDC change is available for the configured range,
   *    some redo logs are missing for the configured range, no dictionary was found (when DICTIONARY_FROM_REDO_LOGS),
   *    database is unavailable, etc. See Table 90-16 in
   *    https://docs.oracle.com/database/121/ARPLS/d_logmnr.htm#ARPLS022 for a complete list of causes.
   */
  public boolean start(LocalDateTime start, LocalDateTime end) {
    Preconditions.checkNotNull(start);
    Preconditions.checkNotNull(end);
    this.startTime = start;
    this.endTime = end;
    String sessionStart = Utils.format(START_TIME_ARG, start.format(dateTimeFormatter));
    String sessionEnd = Utils.format(END_TIME_ARG, end.format(dateTimeFormatter));

    if (!continuousMine) {
      if (!updateLogList(start, end)) {
        return false;
      }
      if (dictionaryFromRedoLogs) {
        // - We cannot use timestamps to define the mining window; we must use SCNs instead. Otherwise an
        //   "ORA-01291: missing logfile" is raised when the mining window reaches the current online log.
        // - We use the adjustMiningWindow* methods to find a valid conversion to SCN limits. The resulting mining
        //   window limits can be slightly different but always cover the original ones and require exactly the same
        //   redo log files.
        BigDecimal firstSCN = adjustMiningWindowLowerLimit(start);
        BigDecimal lastSCN = adjustMiningWindowUpperLimit();
        sessionStart = Utils.format(START_SCN_ARG, firstSCN.toPlainString());
        sessionEnd = Utils.format(END_SCN_ARG, lastSCN.toPlainString());
      }
    }

    LOG.info("Starting LogMiner session for mining window: ({}, {}).", start, this.endTime);
    try (Statement statement = connection.createStatement()) {
      String command = Utils.format(START_LOGMNR_CMD, sessionStart, sessionEnd, this.configOptions);
      statement.execute(command);
      activeSession = true;
    } catch (SQLException e) {
      if (!continuousMine) {
        printCurrentLogs();
      }
      activeSession = false;
      LOG.error(JdbcErrors.JDBC_52.getMessage(), e);
      throw new StageException(
          JdbcErrors.JDBC_52,
          String.format(JDBC_52_LONG_PATTERN,
              "Start",
              e.getMessage(),
              e.getSQLState(),
              e.getErrorCode()
          )
      );
    }

    return true;
  }

  /**
   * Closes the current LogMiner session or do nothing if there was no open session.
   *
   * @throws SQLException The database connection is closed or a database error occurred.
   */
  public void close() throws SQLException {
    if (activeSession) {
      LOG.info("Explicitly closing current LogMiner session.");
      activeSession = false;
      try (Statement statement = connection.createStatement()) {
        statement.execute(STOP_LOGMNR_CMD);
        currentLogList.clear();
      } catch (SQLException e) {
        LOG.error("Could not close LogMiner session: {}", e);
        throw e;
      }
    }
  }

  /**
   * Retrieves CDC records by querying the V$LOGMNR_CONTENTS view. Use this method when COMMITED_DATA_ONLY is not
   * enabled.
   *
   * LogMinerSession must have been started with {@link LogMinerSession#start} before invoking this function. The CDC
   * records returned are limited to the range specified by the {@link LogMinerSession#start} invocation and the
   * arguments passed to this function.
   *
   * If CONTINUOUS_MINE is disabled, use {@link LogMinerSession#isSessionIntegrityGuaranteed} after processing the
   * returned CDC records to ensure there were no data loss situation involved.
   *
   * @param startTime Lower, inclusive limit for the TIMESTAMP column in V$LOGMNR_CONTENTS.
   * @param endTime Upper, exclusive limit for the TIMESTAMP column in V$LOGMNR_CONTENTS.
   * @return A wrapper over the ResultSet with the CDC records retrieved.
   * @throws SQLException LogMinerSession is not active, COMMITED_DATA_ONLY is enabled, or a database connection error
   *     happened.
   */
  public LogMinerResultSetWrapper queryContent(LocalDateTime startTime, LocalDateTime endTime) throws SQLException {
    if (!activeSession) {
      throw new SQLException("No LogMiner session started");
    }
    if (commitedDataOnly) {
      throw new SQLException("Operation only supported when COMMITED_DATA_ONLY is not enabled.");
    }
    queryContentStatement.setString(1, startTime.format(dateTimeFormatter));
    queryContentStatement.setString(2, endTime.format(dateTimeFormatter));
    queryContentStatement.setFetchSize(1);
    return new LogMinerResultSetWrapper(queryContentStatement.executeQuery(), databaseVersion);
  }

  /**
   * Retrieves CDC records by querying the V$LOGMNR_CONTENTS view. Use this method when COMMITED_DATA_ONLY is enabled.
   *
   * LogMinerSession must have been started with {@link LogMinerSession#start} before invoking this function. The CDC
   * records returned are limited to the range specified by the {@link LogMinerSession#start} invocation and the
   * arguments passed to this function.
   *
   * If CONTINUOUS_MINE is disabled, use {@link LogMinerSession#isSessionIntegrityGuaranteed} after processing the
   * returned CDC records to ensure there were no data loss situation involved.
   *
   * @param startTime Lower, inclusive limit for the TIMESTAMP column in V$LOGMNR_CONTENTS.
   * @param endTime Upper, exclusive limit for the TIMESTAMP column in V$LOGMNR_CONTENTS.
   * @param lastCommitSCN Filter for the COMMIT_SCN column in V$LOGMNR_CONTENTS. Rows returned satisfy the condition
   *     (COMMIT_SCN > lastCommitSCN) OR (COMMIT_SCN = lastCommitSCN AND SEQUENCE# > lastSequence).
   * @param lastSequence See {@code lastCommitSCN} above.
   * @return A wrapper over the ResultSet with the CDC records retrieved.
   * @throws SQLException LogMinerSession is not active, COMMITED_DATA_ONLY is not enabled, or a database connection
   *     error happened.
   */
  public LogMinerResultSetWrapper queryContent(LocalDateTime startTime, LocalDateTime endTime,
      BigDecimal lastCommitSCN, int lastSequence) throws SQLException {
    if (!activeSession) {
      throw new SQLException("No LogMiner session started");
    }
    if (!commitedDataOnly) {
      throw new SQLException("Operation only supported when COMMITED_DATA_ONLY is enabled.");
    }

    queryContentStatement.setString(1, startTime.format(dateTimeFormatter));
    queryContentStatement.setString(2, endTime.format(dateTimeFormatter));
    queryContentStatement.setBigDecimal(3, lastCommitSCN);
    queryContentStatement.setInt(4, lastSequence);
    queryContentStatement.setBigDecimal(5, lastCommitSCN);
    if (ddlDictTracking) {
      queryContentStatement.setBigDecimal(6, lastCommitSCN);
    }

    queryContentStatement.setFetchSize(1);
    return new LogMinerResultSetWrapper(queryContentStatement.executeQuery(), databaseVersion);
  }

  /**
   * Check if the active LogMiner session integrity can be guaranteed.
   *
   * When CONTINUOUS_MINE is disabled, the integrity of a LogMiner session can be compromised when one of the online
   * redo logs loaded for mining is overwritten due to a log rotation. Under this scenario, the ResultSet returned by
   * {@link LogMinerSession#queryContent} can silently fail and stop returning results as in a normal "no more pending data"
   * scenario.
   *
   * This method checks the sequence number for the loaded online logs to detect if they have changed since the
   * initialization of the active LogMiner session. If they have changed, then the active LogMiner session could have
   * been compromised and consequently there could be missing CDC records in the returned ResultSet.
   *
   * When CONTINUOUS_MINE option is enabled, then LogMiner actively monitors and internally solves this situation
   * with no active steps required by LogMinerSession. Consequently, this method always returns {@code true}.
   *
   * @return {@code true} if the integrity can be guaranteed, {@code false} otherwise.
   */
  public boolean isSessionIntegrityGuaranteed() {
    if (continuousMine) {
      return true;
    }
    List<RedoLog> updatedOnlineLogs = getOnlineLogs();

    for (RedoLog currentLog : currentLogList) {
      if (currentLog.isOnlineLog()) {
        Optional<RedoLog> updatedLog = updatedOnlineLogs.stream()
            .filter(log -> log.getGroup().equals(currentLog.getGroup()))
            .findFirst();

        if (updatedLog.isPresent()) {
          if (!currentLog.getSequence().equals(updatedLog.get().getSequence())) {
            LOG.debug("Sequence number for {} has changed from {} to {}.",
                currentLog.getPath(), currentLog.getSequence(), updatedLog.get().getSequence());
            return false;
          }
        } else {
          LOG.error(JdbcErrors.JDBC_609.getMessage(), currentLog.getGroup());
          throw new StageException(JdbcErrors.JDBC_609, currentLog.getGroup());
        }
      }
    }
    return true;
  }

  /**
   * Preload the LogMiner dictionary to use in the next sessions.
   *
   * @param dt The starting point where LogMiner will begin to mine. A valid dictionary must be found in a redo log
   *   with FIRST_TIME before {@code dt}.
   * @return True if the dictionary has successfully been loaded. False when CONTINUOUS_MINE option is disabled and a
   *   required redo log is in a transient state (see {@link LogMinerSession#findLogs(BigDecimal, List).
   * @throws StageException No dictionary was found before {@code dt}.
   */
  public boolean preloadDictionary(LocalDateTime dt) {
    LOG.info("Loading LogMiner dictionary for timestamp {}.", dt);
    List<RedoLog> logList = findDictionary(dt);

    if (continuousMine) {
      // Implicitly load the LogMiner dictionary by starting a new LogMiner session for the time period (start, end),
      // where 'start' points to the beginning of the dictionary and 'end' points to the beginning of the redo log
      // covering 'dt'.
      BigDecimal start = logList.get(0).getFirstChange();
      String sessionStart = Utils.format(START_SCN_ARG, start.toPlainString());
      String sessionEnd = Utils.format(END_TIME_ARG, dt.format(dateTimeFormatter));
      String command = Utils.format(START_LOGMNR_CMD, sessionStart, sessionEnd, this.configOptions);

      try (Statement statement = connection.createStatement()) {
        statement.execute(command);
      } catch (SQLException e) {
        LOG.error(JdbcErrors.JDBC_52.getMessage(), e);
        throw new StageException(
            JdbcErrors.JDBC_52,
            String.format(JDBC_52_LONG_PATTERN,
                "Load dictionary",
                e.getMessage(),
                e.getSQLState(),
                e.getErrorCode()
            )
        );
      }

    } else {
      LocalDateTime start = logList.get(logList.size() - 1).getNextTime();
      if (!updateLogList(start, dt)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Utility function that implicitly uses a LogMiner session to find the LocalDateTime corresponding to a given SCN.
   * If the SCN does not exist in the redo logs, the function selects the next closest SCN registered in the redo
   * logs and returns its LocalDateTime.
   *
   * The function might reuse an existing LogMiner session, otherwise it will start a new one. In both cases, after a
   * successful call to this function the LogMiner session will be closed.
   *
   * NOTE: there exists an Oracle SCN_TO_TIMESTAMP function, but when used to return the timestamp for the
   * FIRST_CHANGE# of the oldest redo log, it was observed that the timestamp returned might not match the redo log's
   * FIRST_TIME value. It probably has to do with the undo retention policy in the Oracle database, that affects the
   * domain of SCN values the SCN_TO_TIMESTAMP is able to convert. This motivates the use of this function instead,
   * which only relies on LogMiner.
   *
   * @param scn The queried SCN.
   * @param attempts Number of attempts to find the redo logs covering the queried SCN. An attempt can fail when a redo
   *      log of interest is in a transient state. See {@link LogMinerSession#findLogs(BigDecimal, List).}
   * @return The LocalDateTime for the given SCN.
   * @throws StageException If any error occurs when starting the LogMiner session, no valid SCN was found or
   *      attempts where exhausted.
   */
  public LocalDateTime getLocalDateTimeForSCN(BigDecimal scn, int attempts) throws StageException {
    LOG.debug("Using LogMiner to find timestamp for SCN = {}", scn);
    LocalDateTime result;

    // Find all the redo logs whose range covers the requested SCN.
    List<RedoLog> logList = new ArrayList<>();
    boolean success = false;
    while (!success && attempts > 0) {
      LOG.debug("Attempting to find logs covering SCN = {}", scn);
      success = findLogs(scn, logList);
      attempts--;
      if (!success) {
        LOG.debug("Failed, detected transient state in required redo log. Sleep and retry...");
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }
    }

    if (!success || logList.isEmpty()) {
      String description = Utils.format(
          "Unable to find in the current database incarnation a redo log covering the given SCN (ResetLogsId={}, success={}, logs={})",
          resetLogsId.toPlainString(),
          success,
          logList
      );
      LOG.error(JdbcErrors.JDBC_604.getMessage(), scn.toPlainString(), description);
      throw new StageException(JdbcErrors.JDBC_604, scn.toPlainString(), description);
    }
    logList.sort(Comparator.comparing(RedoLog::getNextChange));
    BigDecimal lastSCN = logList.get(logList.size() - 1).getNextChange();

    for (RedoLog log : logList) {
      if (continuousMine || !currentLogList.contains(log)) {
        String cmd = currentLogList.isEmpty() ? ADD_LOGFILE_NEWLIST_CMD : ADD_LOGFILE_APPEND_CMD;
        try (CallableStatement statement = connection.prepareCall(cmd)) {
          LOG.debug("Add redo log: {}", log);
          statement.setString(1, log.getPath());
          statement.execute();
          currentLogList.add(log);
        } catch (SQLException e) {
          LOG.error("Could not add redo log file to LogMiner: {}", log, e);
        }
      }
    }

    // Start LogMiner session. Unlike the LogMinerSession#start functions, this ad-hoc session simplifies the logic by
    // not honoring the LogMiner options configured via the LogMinerSession.Builder. This can be done as this
    // session is not exposed to the caller and only employed to extract the timestamp for the given SCN.
    try (Statement statement = connection.createStatement()) {
      String sessionStart = Utils.format(START_SCN_ARG, scn.toPlainString());
      String sessionEnd = Utils.format(END_SCN_ARG, lastSCN.toPlainString());
      String command = Utils.format(START_LOGMNR_CMD, sessionStart, sessionEnd, DICT_FROM_ONLINE_CATALOG_OPTION);
      statement.execute(command);
      activeSession = true;
    } catch (SQLException e) {
      LOG.error(JdbcErrors.JDBC_52.getMessage(), e);
      throw new StageException(JdbcErrors.JDBC_52,
          String.format(JDBC_52_LONG_PATTERN,
              "Get local date & time for SCN",
              e.getMessage(),
              e.getSQLState(),
              e.getErrorCode()
          )
      );
    }

    // Query LOGMNR_CONTENTS to find the timestamp for the requested SCN
    try (PreparedStatement statement = connection.prepareStatement(SELECT_TIMESTAMP_FOR_SCN)) {
      statement.setBigDecimal(1, scn);
      statement.setMaxRows(1);
      ResultSet rs = statement.executeQuery();
      if (!rs.next()) {
        String description = "V$LOGMNR_CONTENTS returned no result";
        LOG.error(JdbcErrors.JDBC_604.getMessage(), scn.toPlainString(), description);
        throw new StageException(JdbcErrors.JDBC_604, scn.toPlainString(), description);
      }
      result = rs.getTimestamp(TIMESTAMP_COLUMN).toLocalDateTime();
    } catch (SQLException e) {
      LOG.error(JdbcErrors.JDBC_603.getMessage(), e.getMessage(), e);
      throw new StageException(JdbcErrors.JDBC_603, e);
    }

    try (Statement statement = connection.createStatement()) {
      activeSession = false;
      statement.execute(STOP_LOGMNR_CMD);
      currentLogList.clear();
    } catch (SQLException e) {
      LOG.error("Could not close LogMiner session: {}", e);
    }

    return result;
  }

  public LocalDateTime getStartTime() {
    return this.startTime;
  }

  public LocalDateTime getEndTime() {
    return this.endTime;
  }

  /**
   * Update the V$LOGMNR_LOGS list to cover a given range. The method handles the addition of extra redo logs
   * containing the dictionary (when DICTIONARY_FROM_REDO_LOGS is configured) and any other redo logs required
   * to update the dictionary (when DDL_DICT_TRACKING is also configured).
   *
   * The V$LOGMNR_LOGS list is automatically handled by LogMiner when CONTINUOUS_MINE option is active. So in that
   * case there is no need to use this method.
   *
   * @param startTime The initial SCN to mine.
   * @param endTime The final SCN to mine.
   * @return True if log list was updated, false when it was not possible due to some required redo logs being in a
   *     transient state (i.e. current online log is required but log rotation is in progress, or archiving process
   *     for a relevant log is in progress).
   * @throws StageException No redo log was found covering the specified range or database error.
   */
  private boolean updateLogList(LocalDateTime startTime, LocalDateTime endTime) {
    LocalDateTime start = startTime.truncatedTo(ChronoUnit.SECONDS);
    LocalDateTime end = endTime.truncatedTo(ChronoUnit.SECONDS);
    LOG.debug("Update redo log list for interval: ({}, {}).", start, end);
    List<RedoLog> newLogList = new ArrayList<>();

    if (dictionaryFromRedoLogs) {
      List<RedoLog> dictionary = findDictionary(start);

      if (ddlDictTracking) {
        // When DDL_DICT_TRACKING option is active, we need to readjust the mining window limits to cover all the redo
        // log files touching the time interval (dictionaryBegin, end). This allows LogMiner to complete the dictionary
        // with any DDL transaction registered in these logs.
        start = dictionary.get(0).getFirstTime();  // findDictionary always returns an ordered, non-empty list

        // Update upper limit if the dictionary end is after it (this could happen for an Oracle RAC database).
        LocalDateTime dictEnd = dictionary.get(dictionary.size() - 1).getNextTime();
        if (end.isBefore(dictEnd)) {
          end = dictEnd;
        }

      } else {
        // Just add the redo logs containing the dictionary. The start/end limits must remain unchanged.
        newLogList.addAll(dictionary);
      }
    }

    if (!findLogs(start, end, newLogList)) {
      return false;
    }

    newLogList.sort(Comparator.comparing(RedoLog::getFirstTime));
    if (newLogList.size() == 0 || newLogList.get(0).getFirstTime().compareTo(start) > 0) {
      LOG.warn("Update log list ({}, {}): logs found: {}", start, end, newLogList);
      throw new StageException(JdbcErrors.JDBC_600, start, end);
    }

    Set<String> current = currentLogList.stream().map(log -> log.getPath()).collect(Collectors.toSet());
    Set<String> next = newLogList.stream().map(log -> log.getPath()).collect(Collectors.toSet());

    // Remove online logs and logs no longer needed from the LogMiner internal list (V$LOGMNR_LOGS). We must do
    // it before registering the new redo logs, because if a registered online log has been archived after the last
    // update it will appear in the new list, and trying to add it to LogMiner would result in an ORA-01289 error
    // ("duplicate redo logfile"). Current online, not archived logs are again registered after this.
    for (RedoLog log : currentLogList) {
      if (!next.contains(log.getPath()) || !log.isArchived()) {
        try (CallableStatement statement = connection.prepareCall(REMOVE_LOGFILE_CMD)) {
          LOG.debug("Remove redo log: {}", log);
          statement.setString(1, log.getPath());
          statement.execute();
        } catch (SQLException e) {
          LOG.error("Could not remove redo log {}", log, e);
        }
      }
    }

    currentLogList.clear();

    for (RedoLog log : newLogList) {
      if (current.contains(log.getPath()) && log.isArchived()) {
        currentLogList.add(log);
        LOG.debug("Keeping redo log: {}", log);
      } else {
        String cmd = currentLogList.isEmpty() && current.isEmpty() ? ADD_LOGFILE_NEWLIST_CMD : ADD_LOGFILE_APPEND_CMD;
        try (CallableStatement statement = connection.prepareCall(cmd)) {
          LOG.debug("Add redo log: {}", log);
          statement.setString(1, log.getPath());
          statement.execute();
          currentLogList.add(log);
        } catch (SQLException e) {
          LOG.error("Could not add redo log file to LogMiner: {}", log, e);
        }
      }
    }

    return true;
  }

  /**
   * Returns the list of redo log files containing a valid dictionary for mining from a given starting point.
   *
   * @param start The initial timestamp to mine.
   * @return The list of redo log files with the dictionary, sorted by FIRST_CHANGE# in ascending order.
   * @throws StageException No dictionary was found for the specified timestamp or database error.
   */
  private List<RedoLog> findDictionary(LocalDateTime start) {
    List<RedoLog> result = new ArrayList<>();
    RedoLog dictionaryEnd;

    String query = SELECT_DICTIONARY_END_QUERY
        .replace(SELECT_DICTIONARY_TIME_ARG, dateTimeToString(start))
        .replace(SELECT_DICTIONARY_RESETLOGS_ARG, resetLogsId.toPlainString());

    try (PreparedStatement statement = connection.prepareCall(query)) {
      ResultSet rs = statement.executeQuery();
      if (rs.next()) {
        dictionaryEnd = new RedoLog(
            rs.getString(1),
            rs.getBigDecimal(2),
            rs.getBigDecimal(3),
            rs.getBigDecimal(4),
            rs.getTimestamp(5),
            rs.getTimestamp(6),
            rs.getBigDecimal(7),
            rs.getBigDecimal(8),
            rs.getBoolean(9),
            rs.getBoolean(10),
            rs.getString(11),
            rs.getBoolean(12),
            rs.getBoolean(13)
        );
      } else {
        throw new StageException(JdbcErrors.JDBC_601, Utils.format("no dictionary found before {}", start));
      }
    } catch (SQLException e) {
      throw new StageException(JdbcErrors.JDBC_603, e);
    }

    query = SELECT_DICTIONARY_LOGS_QUERY
        .replace(SELECT_DICTIONARY_SCN_ARG, dictionaryEnd.getFirstChange().toPlainString())
        .replace(SELECT_DICTIONARY_THREAD_ARG, dictionaryEnd.getThread().toPlainString())
        .replace(SELECT_DICTIONARY_RESETLOGS_ARG, resetLogsId.toPlainString());

    try (PreparedStatement statement = connection.prepareCall(query)) {
      ResultSet rs = statement.executeQuery();
      while (rs.next()) {
        RedoLog log = new RedoLog(
            rs.getString(1),
            rs.getBigDecimal(2),
            rs.getBigDecimal(3),
            rs.getBigDecimal(4),
            rs.getTimestamp(5),
            rs.getTimestamp(6),
            rs.getBigDecimal(7),
            rs.getBigDecimal(8),
            rs.getBoolean(9),
            rs.getBoolean(10),
            rs.getString(11),
            rs.getBoolean(12),
            rs.getBoolean(13)
        );
        result.add(log);
      }
    } catch (SQLException e) {
      throw new StageException(JdbcErrors.JDBC_603, e);
    }

    if (result.isEmpty()) {
      throw new StageException(JdbcErrors.JDBC_601, Utils.format("no dictionary found before {}", start));
    }

    return result;
  }

  /**
   * Returns the list of redo logs that covers a given range of changes in database. The range is defined as the
   * time interval [start, end) and the list will contain any redo log covering (partial or completely) that range.
   *
   * @param start Start time (inclusive limit).
   * @param end End time (exclusive limit).
   * @param dest List where the redo logs will be added (append operation).
   * @return True if {@code dest} list was updated, false when it was not possible due to some required redo logs being
   *    in a transient state (i.e. current online log is required but log rotation is in progress, or archiving process
   *    for a relevant log is in progress).
   * @throws StageException Database error.
   */
  private boolean findLogs(LocalDateTime start, LocalDateTime end, List<RedoLog> dest) {
    List<RedoLog> src = new ArrayList<>();

    String query = SELECT_REDO_LOGS_QUERY
        .replace(SELECT_REDO_LOGS_ARG_FIRST, dateTimeToString(start))
        .replace(SELECT_REDO_LOGS_ARG_NEXT, dateTimeToString(end))
        .replace(SELECT_REDO_LOGS_ARG_RESETLOGS, resetLogsId.toPlainString());

    try (PreparedStatement statement = connection.prepareCall(query)) {
      ResultSet rs = statement.executeQuery();
      while (rs.next()) {
        RedoLog log = new RedoLog(
            rs.getString(1),
            rs.getBigDecimal(2),
            rs.getBigDecimal(3),
            rs.getBigDecimal(4),
            rs.getTimestamp(5),
            rs.getTimestamp(6),
            rs.getBigDecimal(7),
            rs.getBigDecimal(8),
            rs.getBoolean(9),
            rs.getBoolean(10),
            rs.getString(11),
            rs.getBoolean(12),
            rs.getBoolean(13)
        );
        src.add(log);
      }
    }
    catch (SQLException e) {
      throw new StageException(JdbcErrors.JDBC_603, e);
    }

    return selectLogs(start, end, src, dest);
  }

  /**
   * Returns the list of redo logs covering a given SCN point in database.
   *
   * @param scn The SCN point in database.
   * @param dest List where the redo logs will be added (append operation). Added logs has FIRST_CHANGE# <= scn,
   *     NEXT_CHANGE > scn.
   * @return True if {@code dest} list was updated, false when it was not possible due to some required redo logs being
   *    in a transient state (i.e. current online log is required but log rotation is in progress, or archiving process
   *    for a relevant log is in progress).
   * @throws StageException Database error.
   */
  private boolean findLogs(BigDecimal scn, List<RedoLog> dest) {
    List<RedoLog> src = new ArrayList<>();
    String query = SELECT_REDO_LOGS_FOR_SCN_QUERY
        .replace(SELECT_REDO_LOGS_FOR_SCN_ARG, scn.toPlainString())
        .replace(SELECT_REDO_LOGS_FOR_SCN_RESETLOGS_ARG, resetLogsId.toPlainString());

    try (PreparedStatement statement = connection.prepareCall(query)) {
      ResultSet rs = statement.executeQuery();
      while (rs.next()) {
        RedoLog log = new RedoLog(
            rs.getString(1),
            rs.getBigDecimal(2),
            rs.getBigDecimal(3),
            rs.getBigDecimal(4),
            rs.getTimestamp(5),
            rs.getTimestamp(6),
            rs.getBigDecimal(7),
            rs.getBigDecimal(8),
            rs.getBoolean(9),
            rs.getBoolean(10),
            rs.getString(11),
            rs.getBoolean(12),
            rs.getBoolean(13)
        );
        src.add(log);
      }
    }
    catch (SQLException e) {
      throw new StageException(JdbcErrors.JDBC_603, e);
    }

    return selectLogs(scn, src, dest);
  }

  /**
   * Returns the list of online redo logs.
   */
  private List<RedoLog> getOnlineLogs() {
    List<RedoLog> src = new ArrayList<>();

    try (PreparedStatement statement = connection.prepareCall(SELECT_ONLINE_REDO_LOGS_QUERY)) {
      ResultSet rs = statement.executeQuery();
      while (rs.next()) {
        RedoLog log = new RedoLog(
            rs.getString(1),
            rs.getBigDecimal(2),
            rs.getBigDecimal(3),
            rs.getBigDecimal(4),
            rs.getTimestamp(5),
            rs.getTimestamp(6),
            rs.getBigDecimal(7),
            rs.getBigDecimal(8),
            rs.getBoolean(9),
            rs.getBoolean(10),
            rs.getString(11),
            rs.getBoolean(12),
            rs.getBoolean(13)
        );
        src.add(log);
      }
    }
    catch (SQLException e) {
      throw new StageException(JdbcErrors.JDBC_603, e);
    }

    return src;
  }

  @VisibleForTesting
  boolean selectLogs(LocalDateTime startTime, LocalDateTime endTime, List<RedoLog> src, List<RedoLog> dest) {
    LocalDateTime start = startTime.truncatedTo(ChronoUnit.SECONDS);
    LocalDateTime end = endTime.truncatedTo(ChronoUnit.SECONDS);
    List<RedoLog> onlineLogs = new ArrayList<>();
    List<RedoLog> selectedLogs = new ArrayList<>();

    for (RedoLog log : src) {
      if (log.isOnlineLog()) {
        onlineLogs.add(log);  // Online logs require further processing.
      } else {
        selectedLogs.add(log);  // All the returned archived logs are required.
        LOG.trace("Find logs ({}, {}): {}, selected", start, end, log);
      }
    }

    if (onlineLogs.isEmpty()) {
      throw new StageException(JdbcErrors.JDBC_603, "no online redo log found.");
    }

    boolean missingArchivedLog = false;
    List<BigDecimal> threads = getThreadsFromRedoLogs(onlineLogs);
    Map<BigDecimal, Boolean> missingCurrentLog = new HashMap<>();
    Map<BigDecimal, Boolean> needsCurrentLog = new HashMap<>();

    for (BigDecimal threadNo: threads) {
      missingCurrentLog.put(threadNo, true);
      needsCurrentLog.put(threadNo, true);
    }

    for (RedoLog log : onlineLogs) {
      boolean overlapLeft = log.getFirstTime().compareTo(start) <= 0 && log.getNextTime().compareTo(start) >= 0;
      boolean overlapRight = log.getFirstTime().compareTo(end) < 0 && log.getNextTime().compareTo(end) >= 0;
      boolean inclusion = log.getFirstTime().compareTo(start) > 0 && log.getNextTime().compareTo(end) < 0;

      if (overlapLeft || overlapRight || inclusion) {
        if (!log.isArchived()) {
          selectedLogs.add(log);
          LOG.trace("Find logs ({}, {}): {}, selected", start, end, log);
        } else {
          missingArchivedLog = selectedLogs.stream().noneMatch(sel -> sel.hasSameData(log));
          if (missingArchivedLog) {
            LOG.warn("Find logs ({}, {}): detected transient state for {} - archiving process still in progress.",
                start, end, log);
            break;
          } else {
            LOG.trace("Find logs ({}, {}): {}, discarded (picking archived copy)", start, end, log);
          }
        }
      } else {
        LOG.trace("Find logs ({}, {}): {}, discarded", start, end, log);
      }
      if (log.getStatus().equals(LOG_STATUS_CURRENT)) {
        missingCurrentLog.put(log.getThread(), false);
      }
      if (log.getNextTime() != null && log.getNextTime().compareTo(end) >= 0) {
        needsCurrentLog.put(log.getThread(), false);  // Time range of interest is before the current redo log range.
      }
    }

    boolean currentLogFailure =
        threads.stream().anyMatch(threadNo -> needsCurrentLog.get(threadNo) && missingCurrentLog.get(threadNo));

    if (currentLogFailure) {
      LOG.warn("Find logs ({}, {}): detected transient state - current log needed but rotation still in progress.",
          start, end);
    }

    boolean statusOK = !currentLogFailure && !missingArchivedLog;
    if (statusOK) {
      dest.addAll(selectedLogs);
    }

    return statusOK;
  }

  @VisibleForTesting
  boolean selectLogs(BigDecimal scn, List<RedoLog> src, List<RedoLog> dest) {
    List<RedoLog> onlineLogs = new ArrayList<>();
    List<RedoLog> selectedLogs = new ArrayList<>();

    for (RedoLog log : src) {
      if (log.isOnlineLog()) {
        onlineLogs.add(log);  // Online logs require further processing.
      } else {
        selectedLogs.add(log);  // All the returned archived logs are required.
        LOG.trace("Find logs ({}): {}, selected", scn, log);
      }
    }

    if (onlineLogs.isEmpty()) {
      throw new StageException(JdbcErrors.JDBC_603, "no online redo log found.");
    }

    boolean missingArchivedLog = false;
    List<BigDecimal> threads = getThreadsFromRedoLogs(onlineLogs);
    Map<BigDecimal, Boolean> missingCurrentLog = new HashMap<>();
    Map<BigDecimal, Boolean> needsCurrentLog = new HashMap<>();

    for (BigDecimal threadNo: threads) {
      missingCurrentLog.put(threadNo, true);
      needsCurrentLog.put(threadNo, true);
    }

    for (RedoLog log : onlineLogs) {
      boolean leftCondition = log.getFirstChange().compareTo(scn) <= 0;
      boolean rightCondition = log.getNextChange().compareTo(scn) >= 0;

      if (leftCondition && rightCondition) {
        if (!log.isArchived()) {
          selectedLogs.add(log);
          LOG.trace("Find logs ({}): {}, selected", scn, log);
        } else {
          missingArchivedLog = selectedLogs.stream().noneMatch(sel -> sel.hasSameData(log));
          if (missingArchivedLog) {
            LOG.warn("Find logs ({}): detected transient state for {}, archiving process still in progress.", scn, log);
            break;
          }
        }
      } else {
        LOG.trace("Find logs ({}): {}, discarded", scn, log);
      }
      if (log.getStatus().equals(LOG_STATUS_CURRENT)) {
        missingCurrentLog.put(log.getThread(), false);
      } else if (log.getNextChange().compareTo(scn) > 0) {
        needsCurrentLog.put(log.getThread(), false);  // Time range of interest is before the current redo log range.
      }
    }

    boolean currentLogFailure =
        threads.stream().anyMatch(threadNo -> needsCurrentLog.get(threadNo) && missingCurrentLog.get(threadNo));

    if (currentLogFailure) {
      LOG.warn("Find logs ({}): detected transient state - current log needed but rotation still in progress.", scn);
    }

    boolean statusOK = !currentLogFailure && !missingArchivedLog;
    if (statusOK) {
      dest.addAll(selectedLogs);
    }

    return statusOK;
  }

  /**
   * Adjusts the lower bound for the upcoming mining window. The new lower bound is returned (as SCN) by the function
   * and, as a side effect, also set (as timestamp) into {@link LogMinerSession#startTime}.
   *
   * @param startTime The initial lower bound to be adjusted.
   *
   * @return The final SCN lower bound.
   */
  private BigDecimal adjustMiningWindowLowerLimit(LocalDateTime startTime) {
    LocalDateTime start = startTime.truncatedTo(ChronoUnit.SECONDS);
    List<BigDecimal> threads = getThreadsFromRedoLogs(this.currentLogList);
    RedoLog selected = null;

    // Find the greatest lower bound for `start`
    for (BigDecimal threadNo : threads) {
      RedoLog candidate = this.currentLogList.stream()
          .filter(l -> l.getThread().equals(threadNo) &&
              l.getFirstTime().compareTo(start) <= 0 &&
              l.getNextTime().compareTo(start) >= 0)
          .min(Comparator.comparing(RedoLog::getNextChange))
          .get();

      if (selected == null || selected.getNextChange().compareTo(candidate.getNextChange()) < 0) {
        selected = candidate;
      }
    }

    this.startTime = selected.getFirstTime();
    return selected.getFirstChange();
  }

  /**
   * Determines the upper bound for the upcoming mining window, when CONTINOUS_MINE option is disabled and dictionary
   * source is DICT_FROM_REDO_LOG. Updates {@link LogMinerSession#endTime} accordingly.
   *
   * @return The SCN upper bound.
   */
  private BigDecimal adjustMiningWindowUpperLimit() {
    // At this point `currentLogList` contains, for each redo thread, the subset of its redo logs covering the interval
    // (dictionary_begin, endTime). Consequently there is one redo log per thread covering the endTime timestamp. We
    // can use as a valid SCN upper limit the lowest NEXT_CHANGE# from those redo logs. That ensures:
    // 1) endTime <= upperLimit; which is required to cover the mining window (dictionary_begin, endTime).
    // 2) upperLimit <= NEXT_CHANGE# for all the redo logs covering endTime; otherwise, an "ORA-01291: missing log
    //    file error" will be raised because the final mining window (dictionary_begin, upperLimit) must be covered for
    //    each thread with the redologs in `currentLogList`.
    //
    // NOTE: we avoid to use TIMESTAMP_TO_SCN(endTime) as the upper limit, as the TIMESTAMP_TO_SCN oracle function
    // depends on undo retention policies to work fine. See the official documentation for more details.

    List<BigDecimal> threads = getThreadsFromRedoLogs(this.currentLogList);
    RedoLog selected = null;

    // Find the lowest upper bound.
    for (BigDecimal threadNo : threads) {
      RedoLog candidate = this.currentLogList.stream()
                                             .filter(log -> log.getThread().equals(threadNo))
                                             .max(Comparator.comparing(RedoLog::getNextChange))
                                             .get();

      if (selected == null || selected.getNextChange().compareTo(candidate.getNextChange()) > 0) {
        selected = candidate;
      }
    }

    this.endTime = selected.getNextTime();
    return selected.getNextChange();
  }

  /**
   * Queries Oracle to get the ResetLogs ID for the current database incarnation.
   *
   * @return The current ResetLogs Id.
   * @throws StageException The database query returned no results or an error.
   */
  private BigDecimal queryCurrentResetLogsId() {
    BigDecimal resetlogsId;

    try (PreparedStatement statement = connection.prepareCall(SELECT_CURRENT_DATABASE_INCARNATION_QUERY)) {
      ResultSet rs = statement.executeQuery();
      if (!rs.next()) {
        String message = "Query returned no rows";
        LOG.error(JdbcErrors.JDBC_610.getMessage(), message);
        throw new StageException(JdbcErrors.JDBC_610, message);
      }
      resetlogsId = rs.getBigDecimal(1);
    } catch (SQLException e) {
      LOG.error(JdbcErrors.JDBC_610.getMessage(), e.getMessage());
      throw new StageException(JdbcErrors.JDBC_610, e.getMessage());
    }

    return resetlogsId;
  }

  private void decideEmptyStringEqualsNull() {
    try (PreparedStatement statement = connection.prepareStatement(CHECK_EMPTY_STRING_EQUALS_NULL_QUERY)) {
      ResultSet result = statement.executeQuery();
      this.emptyStringEqualsNull = result.next();
    } catch (SQLException eSQLException) {
      throw new StageException(JdbcErrors.JDBC_608, eSQLException.getErrorCode());
    }
  }

  public boolean isEmptyStringEqualsNull() {
     return this.emptyStringEqualsNull;
  }

  private List<BigDecimal> getThreadsFromRedoLogs(List<RedoLog> logs) {
    return logs.stream()
               .map(log -> log.getThread())
               .distinct()
               .collect(Collectors.toList());
  }

  private String dateTimeToString(LocalDateTime dt) {
    return Utils.format("TO_DATE('{}', '{}')", dt.format(dateTimeFormatter), TO_DATE_FORMAT);
  }

  /**
   * Writes in log the list of redo logs registered in V$LOGMNR_LOGS. This can be handy for debugging and comparing
   * with respect to the internal list {@code currentLogList} kept by this {@link LogMinerSession}.
   */
  private void printCurrentLogs() {
    LOG.debug("Listing current logs registered in V$LOGMNR_LOGS:");
    try (PreparedStatement statement = connection.prepareCall(SELECT_LOGMNR_LOGS_QUERY)) {
      ResultSet rs = statement.executeQuery();
      ResultSetMetaData metaData = rs.getMetaData();
      int columnCount = metaData.getColumnCount();
      String logLine;
      while (rs.next()) {
        logLine = "V$LOGMNR_LOGS: ";
        for (int i = 0; i < columnCount; i++) {
          logLine += metaData.getColumnName(i) + "->" + rs.getString(i) + "; ";
        }
        LOG.info(logLine);
      }
    } catch (SQLException e) {
      LOG.error("Failed to query V$LOGMNR_LOGS: {}", e.getMessage());
    }
  }

}
