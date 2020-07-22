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
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
      + " XIDUSN, XIDSLT, XIDSQN, RS_ID, SSN, SEG_OWNER, ROLLBACK, ROW_ID "
      + " FROM V$LOGMNR_CONTENTS "
      + " WHERE {}";
  private static final String COMMIT_SCN_COLUMN = "COMMIT_SCN";
  private static final String COMMIT_SCN_COLUMN_OLD = "CSCN";  // Oracle version < 11

  // Templates to manually manage the LogMiner redo log list.
  private static final String ADD_LOGFILE_NEWLIST_CMD = "BEGIN DBMS_LOGMNR.ADD_LOGFILE(?, DBMS_LOGMNR.NEW); END;";
  private static final String ADD_LOGFILE_APPEND_CMD = "BEGIN DBMS_LOGMNR.ADD_LOGFILE(?, DBMS_LOGMNR.ADDFILE); END;";
  private static final String REMOVE_LOGFILE_CMD = "BEGIN DBMS_LOGMNR.REMOVE_LOGFILE(?); END;";

  // Template to retrieve the list of redo log files given a SCN range (first, next) of interest.
  private static final String SELECT_REDO_LOGS_QUERY =
      " SELECT NAME, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, DICTIONARY_BEGIN, "
      + "    DICTIONARY_END, 'YES' AS ARCHIVED "
      + " FROM V$ARCHIVED_LOG "
      + " WHERE STATUS = 'A' AND "
      + "     ((FIRST_CHANGE# <= :first AND NEXT_CHANGE# > :first) OR "
      + "      (FIRST_CHANGE# < :next AND NEXT_CHANGE# >= :next) OR "
      + "      (FIRST_CHANGE# > :first AND NEXT_CHANGE# < :next)) "
      + " UNION "
      + " SELECT VLOGFILE.MEMBER, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, "
      + "     'NO' AS DICTIONARY_BEGIN, 'NO' AS DICTIONARY_END, ARCHIVED "
      + " FROM V$LOG, "
      + "     (SELECT GROUP#, MEMBER, ROW_NUMBER() OVER (PARTITION BY GROUP# ORDER BY GROUP#) AS ROWNO "
      + "      FROM V$LOGFILE) VLOGFILE "
      + " WHERE V$LOG.GROUP# = VLOGFILE.GROUP# AND V$LOG.MEMBERS = VLOGFILE.ROWNO AND V$LOG.ARCHIVED = 'NO' AND "
      + "     ((FIRST_CHANGE# <= :first AND NEXT_CHANGE# > :first) OR "
      + "      (FIRST_CHANGE# < :next AND NEXT_CHANGE# >= :next) OR "
      + "      (FIRST_CHANGE# > :first AND NEXT_CHANGE# < :next)) ";
  private static final String SELECT_REDO_LOGS_ARG_FIRST = ":first";
  private static final String SELECT_REDO_LOGS_ARG_NEXT = ":next";

  // Templates to retrieve the redo logs containing the LogMiner dictionary valid for transactions with SCN >= :scn.
  private static final String SELECT_DICTIONARY_END_QUERY =
      " SELECT NAME, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, DICTIONARY_BEGIN, "
      + "    DICTIONARY_END, 'YES' AS ARCHIVED "
      + " FROM V$ARCHIVED_LOG "
      + " WHERE FIRST_CHANGE# = (SELECT MAX(FIRST_CHANGE#) FROM V$ARCHIVED_LOG WHERE STATUS = 'A' AND "
      + "                        DICTIONARY_END = 'YES' AND FIRST_CHANGE# <= :scn) ";
  private static final String SELECT_DICTIONARY_LOGS_QUERY =
      " SELECT NAME, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, DICTIONARY_BEGIN, "
      + "    DICTIONARY_END, 'YES' AS ARCHIVED "
      + " FROM V$ARCHIVED_LOG "
      + " WHERE FIRST_CHANGE# >= (SELECT MAX(FIRST_CHANGE#) FROM V$ARCHIVED_LOG WHERE STATUS = 'A' AND "
      + "                         DICTIONARY_BEGIN = 'YES' AND FIRST_CHANGE# <= :scn AND THREAD# = :thread) AND "
      + "    FIRST_CHANGE# <= :scn AND STATUS = 'A' AND THREAD# = :thread "
      + " ORDER BY FIRST_CHANGE# ";
  private static final String SELECT_DICTIONARY_SCN_ARG = ":scn";
  private static final String SELECT_DICTIONARY_THREAD_ARG = ":thread";

  // Template to retrieve all the redo log files with data for a specific point in time.
  private static final String SELECT_LOGS_FROM_DATE_QUERY =
      " SELECT NAME, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, DICTIONARY_BEGIN, "
      + "     DICTIONARY_END, 'YES' AS ARCHIVED "
      + " FROM V$ARCHIVED_LOG "
      + " WHERE STATUS = 'A' AND FIRST_TIME <= :time AND NEXT_TIME > :time "
      + " UNION "
      + " SELECT VLOGFILE.MEMBER, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, "
      + "     'NO' AS DICTIONARY_BEGIN, 'NO' AS DICTIONARY_END, ARCHIVED  "
      + " FROM V$LOG, "
      + "     (SELECT GROUP#, MEMBER, ROW_NUMBER() OVER (PARTITION BY GROUP# ORDER BY GROUP#) AS ROWNO "
      + "      FROM V$LOGFILE) VLOGFILE "
      + " WHERE V$LOG.GROUP# = VLOGFILE.GROUP# AND V$LOG.MEMBERS = VLOGFILE.ROWNO AND V$LOG.ARCHIVED = 'NO' AND "
      + "     FIRST_TIME <= :time AND (NEXT_TIME > :time OR NEXT_TIME IS NULL) ";
  private static final String SELECT_LOGS_FROM_DATE_ARG = ":time";

  // Template for the getLocalDateTimeForSCN utility function.
  private static final String SELECT_TIMESTAMP_FOR_SCN =
      "SELECT TIMESTAMP FROM V$LOGMNR_CONTENTS WHERE SCN >= ? ORDER BY SCN";
  private static final String TIMESTAMP_COLUMN = "TIMESTAMP";

  // Query retrieving the current redo log files registered in the current LogMiner session.
  private final String SELECT_LOGMNR_LOGS_QUERY = "SELECT FILENAME FROM V$LOGMNR_LOGS ORDER BY LOW_SCN";

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
  private boolean activeSession;

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

      return new LogMinerSession(connection, databaseVersion, createQueryContentStatement(), configOptions);
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
        String conditions = Utils.format("TIMESTAMP >= ? AND {} AND {} AND ({})",
            tablesCondition, opsCondition, restartCondition);

        result = connection.prepareStatement(Utils.format(SELECT_LOGMNR_CONTENT_QUERY, commitScnColumn, conditions));

      } else {
        String tnxOps = Utils.format("OPERATION_CODE IN ({},{})", COMMIT_CODE, ROLLBACK_CODE);
        String conditions = Utils.format("TIMESTAMP >= ? AND (({} AND {}) OR {})",
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
   * @throws StageException If an invalid range was configured, no CDC change is available for the configured range,
   *    some redo logs are missing for the configured range, no dictionary was found (when DICTIONARY_FROM_REDO_LOGS),
   *    database is unavailable, etc. See Table 90-16 in
   *    https://docs.oracle.com/database/121/ARPLS/d_logmnr.htm#ARPLS022 for a complete list of causes.
   */
  public void start(LocalDateTime start, LocalDateTime end) {
    Preconditions.checkNotNull(start);
    Preconditions.checkNotNull(end);
    String sessionStart = Utils.format(START_TIME_ARG, start.format(dateTimeFormatter));
    String sessionEnd = Utils.format(END_TIME_ARG, end.format(dateTimeFormatter));
    LOG.info("Starting LogMiner session for mining window: ({}, {}).", start, end);

    if (!continuousMine) {
      BigDecimal firstSCN = findEarliestLogCoveringDateTime(start).getFirstChange();
      BigDecimal lastSCN = findLatestLogCoveringDateTime(end).getNextChange();
      updateLogList(firstSCN, lastSCN);

      if (dictionaryFromRedoLogs) {
        // When DICT_FROM_REDO_LOGS option is enabled, avoiding to use Timestamps to define the mining window and
        // using SCNs instead. Otherwise an "ORA-01291: missing logfile" is raised when the mining window reaches the
        // current online log.
        sessionStart = Utils.format(START_SCN_ARG, firstSCN.toPlainString());
        sessionEnd = Utils.format(END_SCN_ARG, lastSCN.toPlainString());
      }
    }

    try (Statement statement = connection.createStatement()) {
      String command = Utils.format(START_LOGMNR_CMD, sessionStart, sessionEnd, this.configOptions);
      statement.execute(command);
      activeSession = true;
    } catch (SQLException e) {
      if (!continuousMine) {
        printCurrentLogs();
      }
      activeSession = false;
      throw new StageException(JdbcErrors.JDBC_52, e);
    }
  }

  /**
   * Starts a new LogMiner session. This closes the currently active session, if any.
   * The {@code start} and {@code end} parameters define the range of redo records accessible in this session for
   * mining.
   *
   * @param start Available redo records will have a SCN greater than or equal to {@code start}.
   * @param end Available redo records will have a SCN less than {@code end}.
   * @throws StageException If an invalid range was configured, no CDC change is available for the configured range,
   *    some redo logs are missing for the configured range, no dictionary was found (when DICTIONARY_FROM_REDO_LOGS),
   *    database is unavailable, etc. See Table 90-16 in
   *    https://docs.oracle.com/database/121/ARPLS/d_logmnr.htm#ARPLS022 for a complete list of causes.
   */
  public void start(BigDecimal start, BigDecimal end) {
    Preconditions.checkNotNull(start);
    Preconditions.checkNotNull(end);
    String sessionStart = Utils.format(START_SCN_ARG, start.toPlainString());
    String sessionEnd = Utils.format(END_SCN_ARG, end.toPlainString());
    LOG.info("Starting LogMiner session for mining window: ({}, {}).", start, end);

    if (!continuousMine) {
      updateLogList(start, end);
    }

    try (Statement statement = connection.createStatement()) {
      String command = Utils.format(START_LOGMNR_CMD, sessionStart, sessionEnd, this.configOptions);
      statement.execute(command);
      activeSession = true;
    } catch (SQLException e) {
      if (!continuousMine) {
        printCurrentLogs();
      }
      activeSession = false;
      throw new StageException(JdbcErrors.JDBC_52, e);
    }
  }

  /**
   * Starts a new LogMiner session. This closes the currently active session, if any.
   *
   * The {@code start} and {@code end} parameters define the range of redo records accessible in this session for
   * mining. When CONTINUOUS_MINE is not active and DICT_FROM_REDO_LOGS is enabled, the {@code start} is internally
   * adjusted to the FIRST_TIME of the oldest redo log containing relevant data.
   *
   * @param start The initial point where LogMiner will start to mine at. If CONTINUOUS_MINE is not active and
   *    DICT_FROM_REDO_LOGS is enabled, the function shifts backwards the {@code start} to the FIRST_TIME of the oldest
   *    redo log with relevant data. Available redo records will have a timestamp later than or equal to (the
   *    adjusted) {@code start}.
   * @param end The last SCN where LogMiner will stop to mine at. Available redo records will have a SCN less than
   *   {@code end}.
   * @throws StageException If an invalid range was configured, no CDC change is available for the configured range,
   *    some redo logs are missing for the configured range, no dictionary was found (when DICTIONARY_FROM_REDO_LOGS),
   *    database is unavailable, etc. See Table 90-16 in
   *    https://docs.oracle.com/database/121/ARPLS/d_logmnr.htm#ARPLS022 for a complete list of causes.
   */
  public void start(LocalDateTime start, BigDecimal end) {
    Preconditions.checkNotNull(start);
    Preconditions.checkNotNull(end);
    String sessionStart = Utils.format(START_TIME_ARG, start.format(dateTimeFormatter));
    String sessionEnd = Utils.format(END_SCN_ARG, end.toPlainString());
    LOG.info("Starting LogMiner session for mining window: ({}, {}).", start, end);

    if (!continuousMine) {
      BigDecimal firstSCN = findEarliestLogCoveringDateTime(start).getFirstChange();
      updateLogList(firstSCN, end);
      if (dictionaryFromRedoLogs) {
        // When DICT_FROM_REDO_LOGS option is enabled, avoiding to use Timestamps to define the mining window and
        // using SCNs instead. Otherwise an "ORA-01291: missing logfile" is raised when the mining window reaches an
        // online log.
        sessionStart = Utils.format(START_SCN_ARG, firstSCN.toPlainString());
      }
    }

    try (Statement statement = connection.createStatement()) {
      String command = Utils.format(START_LOGMNR_CMD, sessionStart, sessionEnd, this.configOptions);
      statement.execute(command);
      activeSession = true;
    } catch (SQLException e) {
      if (!continuousMine) {
        printCurrentLogs();
      }
      activeSession = false;
      throw new StageException(JdbcErrors.JDBC_52, e);
    }
  }

  /**
   * Starts a new LogMiner session. This closes the currently active session, if any.
   *
   * The {@code start} and {@code end} parameters define the range of redo records accessible in this session for
   * mining. When CONTINUOUS_MINE is not active and DICT_FROM_REDO_LOGS is enabled, {@code end} is internally
   * adjusted to the NEXT_TIME of the most recent redo log containing relevant data.
   *
   * @param start The initial SCN where LogMiner will start to mine at. Available redo records will have a SCN
   *    greater than or equal to {@code start}.
   * @param end The last point where LogMiner will stop to mine at. If CONTINUOUS_MINE is not active and
   *    DICT_FROM_REDO_LOGS is enabled, the function shifts the {@code stop} to the NEXT_TIME of the most recent redo
   *    log with relevant data. Available redo records will have a datetime earlier than (the adjusted) {@code end}.
   * @throws StageException If an invalid range was configured, no CDC change is available for the configured range,
   *    some redo logs are missing for the configured range, no dictionary was found (when DICTIONARY_FROM_REDO_LOGS),
   *    database is unavailable, etc. See Table 90-16 in
   *    https://docs.oracle.com/database/121/ARPLS/d_logmnr.htm#ARPLS022 for a complete list of causes.
   */
  public void start(BigDecimal start, LocalDateTime end) {
    Preconditions.checkNotNull(start);
    Preconditions.checkNotNull(end);
    String sessionStart = Utils.format(START_SCN_ARG, start.toPlainString());
    String sessionEnd = Utils.format(END_TIME_ARG, end.format(dateTimeFormatter));
    LOG.info("Starting LogMiner session for mining window: ({}, {}).", start, end);

    if (!continuousMine) {
      BigDecimal lastSCN = findLatestLogCoveringDateTime(end).getNextChange();
      updateLogList(start, lastSCN);
      if (dictionaryFromRedoLogs) {
        // When DICT_FROM_REDO_LOGS option is enabled, avoiding to use Timestamps to define the mining window and
        // using SCNs instead. Otherwise an "ORA-01291: missing logfile" is raised when the mining window reaches an
        // online log.
        sessionEnd = Utils.format(END_SCN_ARG, lastSCN.toPlainString());
      }
    }

    try (Statement statement = connection.createStatement()) {
      String command = Utils.format(START_LOGMNR_CMD, sessionStart, sessionEnd, this.configOptions);
      statement.execute(command);
      activeSession = true;
    } catch (SQLException e) {
      if (!continuousMine) {
        printCurrentLogs();
      }
      activeSession = false;
      throw new StageException(JdbcErrors.JDBC_52, e);
    }
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
   * @param startTime Lower, inclusive limit for the TIMESTAMP column in V$LOGMNR_CONTENTS.
   * @return A wrapper over the ResultSet with the CDC records retrieved.
   * @throws SQLException LogMinerSession is not active, COMMITED_DATA_ONLY is enabled, or a database connection error
   *     happened.
   */
  public LogMinerResultSetWrapper queryContent(LocalDateTime startTime) throws SQLException {
    if (!activeSession) {
      throw new SQLException("No LogMiner session started");
    }
    if (commitedDataOnly) {
      throw new SQLException("Operation only supported when COMMITED_DATA_ONLY is not enabled.");
    }
    queryContentStatement.setString(1, startTime.format(dateTimeFormatter));
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
   * @param startTime Lower, inclusive limit for the TIMESTAMP column in V$LOGMNR_CONTENTS.
   * @param lastCommitSCN Filter for the COMMIT_SCN column in V$LOGMNR_CONTENTS. Rows returned satisfy the condition
   *     (COMMIT_SCN > lastCommitSCN) OR (COMMIT_SCN = lastCommitSCN AND SEQUENCE# > lastSequence).
   * @param lastSequence See {@code lastCommitSCN} above.
   * @return A wrapper over the ResultSet with the CDC records retrieved.
   * @throws SQLException LogMinerSession is not active, COMMITED_DATA_ONLY is not enabled, or a database connection
   *     error happened.
   */
  public LogMinerResultSetWrapper queryContent(LocalDateTime startTime, BigDecimal lastCommitSCN, int lastSequence) throws SQLException {
    if (!activeSession) {
      throw new SQLException("No LogMiner session started");
    }
    if (!commitedDataOnly) {
      throw new SQLException("Operation only supported when COMMITED_DATA_ONLY is enabled.");
    }

    queryContentStatement.setString(1, startTime.format(dateTimeFormatter));
    queryContentStatement.setBigDecimal(2, lastCommitSCN);
    queryContentStatement.setInt(3, lastSequence);
    queryContentStatement.setBigDecimal(4, lastCommitSCN);
    if (ddlDictTracking) {
      queryContentStatement.setBigDecimal(5, lastCommitSCN);
    }

    queryContentStatement.setFetchSize(1);
    return new LogMinerResultSetWrapper(queryContentStatement.executeQuery(), databaseVersion);
  }

  /**
   * Preload the LogMiner dictionary to use in the next sessions.
   *
   * @param dt The starting point where LogMiner will begin to mine. A valid dictionary must be found in a redo log
   *   with FIRST_TIME before {@code dt}.
   * @throws StageException No dictionary was found before {@code dt}.
   */
  public void preloadDictionary(LocalDateTime dt) {
    RedoLog log = findEarliestLogCoveringDateTime(dt);
    preloadDictionary(log.getFirstChange());
  }

  /**
   * Preload the LogMiner dictionary to use in the next sessions.
   *
   * @param scn The starting point where LogMiner will begin to mine. A valid dictionary must be found in a redo log
   *   with FIRST_CHANGE# before {@code scn}.
   * @throws StageException No dictionary was found before {@code scn} or database error.
   */
  public void preloadDictionary(BigDecimal scn) {
    List<RedoLog> logList = findDictionary(scn);

    if (continuousMine) {
      BigDecimal start = logList.get(0).getFirstChange();
      start(start, scn);  // Force the dictionary extraction by starting a LogMiner session with a proper range.
    } else {
      BigDecimal start = logList.get(logList.size() - 1).getNextChange();
      updateLogList(start, scn);
    }
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
   * @return The LocalDateTime for the given SCN.
   * @throws StageException If any error occurs when starting the LogMiner session or no valid SCN was found.
   */
  public LocalDateTime getLocalDateTimeForSCN(BigDecimal scn) throws StageException {
    LOG.debug("Using LogMiner to find timestamp for SCN = {}", scn);
    LocalDateTime result;

    // Find all the redo logs whose range covers the requested SCN.
    List<RedoLog> logList = findLogs(scn, scn.add(BigDecimal.valueOf(1)));
    if (logList.isEmpty()) {
      throw new StageException(JdbcErrors.JDBC_604, scn.toPlainString());
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
      throw new StageException(JdbcErrors.JDBC_52, e);
    }

    // Query LOGMNR_CONTENTS to find the timestamp for the requested SCN
    try (PreparedStatement statement = connection.prepareStatement(SELECT_TIMESTAMP_FOR_SCN)) {
      statement.setBigDecimal(1, scn);
      statement.setMaxRows(1);
      ResultSet rs = statement.executeQuery();
      if (!rs.next()) {
        throw new StageException(JdbcErrors.JDBC_604, scn.toPlainString());
      }
      result = rs.getTimestamp(TIMESTAMP_COLUMN).toLocalDateTime();
    } catch (SQLException e) {
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

  /**
   * Finds the earliest log that were active at a given point in time, where the earliest log is the one with the
   * lowest FIRST_CHANGE# value.
   *
   * @throws StageException No redo log was found covering the specified datetime or database error.
   */
  private RedoLog findEarliestLogCoveringDateTime(LocalDateTime dt) {
    List<RedoLog> logs = findLogsCoveringDateTime(dt, true);
    return logs.get(0);
  }

  /**
   * Finds the latest log that were active at a given point in time, where the latest log is the one with the highest
   * NEXT_CHANGE# value.
   *
   * @throws StageException No redo log was found covering the specified datetime or database error.
   */
  private RedoLog findLatestLogCoveringDateTime(LocalDateTime dt) {
    List<RedoLog> logs = findLogsCoveringDateTime(dt, false);
    return logs.get(logs.size() - 1);
  }

  /**
   * Finds all the logs that were active during a given point in time.
   *
   * @param dt The point in time to look for.
   * @param sortByFirstChange If true, the log list is sorted by FIRST_CHANGE#, otherwise is sorted by NEXT_CHANGE#.
   * @return The list containing all the redo logs with FIRT_TIME <= dt < NEXT_TIME.
   * @throws StageException No redo log was found covering the specified range or database error.
   */
  private List<RedoLog> findLogsCoveringDateTime(LocalDateTime dt, boolean sortByFirstChange) {
    List<RedoLog> result = new ArrayList<>();
    String query = SELECT_LOGS_FROM_DATE_QUERY.replace(SELECT_LOGS_FROM_DATE_ARG, dateTimeToString(dt));
    try (PreparedStatement statement = connection.prepareCall(query)) {
      ResultSet rs = statement.executeQuery();
      while (rs.next()) {
        RedoLog log = new RedoLog(
            rs.getString(1),
            rs.getBigDecimal(2),
            rs.getBigDecimal(3),
            rs.getTimestamp(4),
            rs.getTimestamp(5),
            rs.getBigDecimal(6),
            rs.getBigDecimal(7),
            rs.getBoolean(8),
            rs.getBoolean(9),
            rs.getBoolean(10)
        );
        result.add(log);
      }
    } catch (SQLException e) {
      throw new StageException(JdbcErrors.JDBC_603, e);
    }

    if (result.isEmpty()) {
      throw new StageException(JdbcErrors.JDBC_602, dt.format(dateTimeFormatter));
    }
    if (sortByFirstChange) {
      result.sort(Comparator.comparing(RedoLog::getFirstChange));
    } else {
      result.sort(Comparator.comparing(RedoLog::getNextChange));
    }
    return result;
  }

  /**
   * Update the V$LOGMNR_LOGS list to cover a given range. The method handles the addition of extra redo logs
   * containing the dictionary (when DICTIONARY_FROM_REDO_LOGS is configured) and any other redo logs required
   * to update the dictionary (when DDL_DICT_TRACKING is also configured).
   *
   * The V$LOGMNR_LOGS list is automatically handled by LogMiner when CONTINUOUS_MINE option is active. So in that
   * case there is no need to use this method.
   *
   * @param start The initial SCN to mine.
   * @param end The final SCN to mine.
   * @throws StageException No redo log was found covering the specified range or database error.
   */
  private void updateLogList(BigDecimal start, BigDecimal end) {
    List<RedoLog> newLogList = new ArrayList<>();

    if (dictionaryFromRedoLogs) {
      newLogList.addAll(findDictionary(start));
      if (ddlDictTracking) {
        // When DDL_DICT_TRACKING option is active, we additionally need all the redo log files existing after the
        // dictionary and until the beginning of the mining window. This allows LogMiner to complete the dictionary
        // with any DDL transaction registered in these logs.
        start = newLogList.get(newLogList.size() - 1).getNextChange();
      }
    }
    newLogList.addAll(findLogs(start, end));
    newLogList.sort(Comparator.comparing(RedoLog::getFirstChange));

    if (newLogList.size() == 0 || newLogList.get(0).getFirstChange().compareTo(start) > 0) {
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
  }

  /**
   * Returns the list of redo log files containing a valid dictionary for mining from a given starting point.
   *
   * @param start The initial SCN to mine.
   * @return The list of redo log files with the dictionary, sorted by FIRST_CHANGE# in ascending order.
   * @throws StageException No dictionary was found for the specified SCN or database error.
   */
  private List<RedoLog> findDictionary(BigDecimal start) {
    List<RedoLog> result = new ArrayList<>();
    RedoLog dictionaryEnd;

    String query = SELECT_DICTIONARY_END_QUERY.replace(SELECT_DICTIONARY_SCN_ARG, start.toPlainString());
    try (PreparedStatement statement = connection.prepareCall(query)) {
      ResultSet rs = statement.executeQuery();
      if (rs.next()) {
        dictionaryEnd = new RedoLog(
            rs.getString(1),
            rs.getBigDecimal(2),
            rs.getBigDecimal(3),
            rs.getTimestamp(4),
            rs.getTimestamp(5),
            rs.getBigDecimal(6),
            rs.getBigDecimal(7),
            rs.getBoolean(8),
            rs.getBoolean(9),
            rs.getBoolean(10)
        );
      } else {
        throw new StageException(JdbcErrors.JDBC_601, Utils.format("no dictionary found before SCN {}", start));
      }
    } catch (SQLException e) {
      throw new StageException(JdbcErrors.JDBC_603, e);
    }

    query = SELECT_DICTIONARY_LOGS_QUERY
        .replace(SELECT_DICTIONARY_SCN_ARG, dictionaryEnd.getFirstChange().toPlainString())
        .replace(SELECT_DICTIONARY_THREAD_ARG, dictionaryEnd.getThread().toPlainString());

    try (PreparedStatement statement = connection.prepareCall(query)) {
      ResultSet rs = statement.executeQuery();
      if (rs.next()) {
        RedoLog log = new RedoLog(
            rs.getString(1),
            rs.getBigDecimal(2),
            rs.getBigDecimal(3),
            rs.getTimestamp(4),
            rs.getTimestamp(5),
            rs.getBigDecimal(6),
            rs.getBigDecimal(7),
            rs.getBoolean(8),
            rs.getBoolean(9),
            rs.getBoolean(10)
        );
        result.add(log);
      } else {
        throw new StageException(JdbcErrors.JDBC_601, Utils.format("no dictionary found before SCN {}", start));
      }
    } catch (SQLException e) {
      throw new StageException(JdbcErrors.JDBC_603, e);
    }

    return result;
  }

  /**
   * Returns the list of redo logs that covers a given range of changes in database. The range is defined as the
   * interval [start, end) and the list will contain any redo log with some SCN in that range; i.e. it is not necessary
   * that all the redo log SCNs are in range [start, end).
   *
   * @param start Start SCN (inclusive limit).
   * @param end End SCN (exclusive limit).
   * @return List of all the redo logs having some SCN in range [start, end).
   * @throws StageException Database error.
   */
  private List<RedoLog> findLogs(BigDecimal start, BigDecimal end) {
    List<RedoLog> result = new ArrayList<>();
    String query = SELECT_REDO_LOGS_QUERY
        .replace(SELECT_REDO_LOGS_ARG_FIRST, start.toPlainString())
        .replace(SELECT_REDO_LOGS_ARG_NEXT, end.toPlainString());

    try (PreparedStatement statement = connection.prepareCall(query)) {
      ResultSet rs = statement.executeQuery();
      while (rs.next()) {
        RedoLog log = new RedoLog(
            rs.getString(1),
            rs.getBigDecimal(2),
            rs.getBigDecimal(3),
            rs.getTimestamp(4),
            rs.getTimestamp(5),
            rs.getBigDecimal(6),
            rs.getBigDecimal(7),
            rs.getBoolean(8),
            rs.getBoolean(9),
            rs.getBoolean(10)
        );
        result.add(log);
      }
    }
    catch (SQLException e) {
      throw new StageException(JdbcErrors.JDBC_603, e);
    }

    return result;
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
      while (rs.next()) {
        LOG.info("V$LOGMNR_LOGS: {}", rs.getString(1));
      }
    } catch (SQLException e) {
      LOG.error("Failed to query V$LOGMNR_LOGS: {}", e.getMessage());
    }
  }

}
