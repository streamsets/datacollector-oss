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

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
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
import java.util.stream.Collectors;


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

  // Templates to manually manage the LogMiner redo log list.
  private static final String ADD_LOGFILE_NEWLIST_CMD = "BEGIN DBMS_LOGMNR.ADD_LOGFILE(?, DBMS_LOGMNR.NEW); END;";
  private static final String ADD_LOGFILE_APPEND_CMD = "BEGIN DBMS_LOGMNR.ADD_LOGFILE(?, DBMS_LOGMNR.ADDFILE); END;";
  private static final String REMOVE_LOGFILE_CMD = "BEGIN DBMS_LOGMNR.REMOVE_LOGFILE(?); END;";

  // Template to retrieve the list of redo log files given a SCN range (first, next) of interest.
  private static final String SELECT_REDO_LOGS_QUERY =
      " SELECT NAME, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, DICTIONARY_BEGIN, "
      + "    DICTIONARY_END "
      + " FROM V$ARCHIVED_LOG "
      + " WHERE STATUS = 'A' AND "
      + "     ((FIRST_CHANGE# <= :first AND NEXT_CHANGE# > :first) OR "
      + "      (FIRST_CHANGE# < :next AND NEXT_CHANGE# >= :next) OR "
      + "      (FIRST_CHANGE# > :first AND NEXT_CHANGE# < :next)) "
      + " UNION "
      + " SELECT VLOGFILE.MEMBER, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, "
      + "     'NO' AS DICTIONARY_BEGIN, 'NO' AS DICTIONARY_END "
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
      + "    DICTIONARY_END "
      + " FROM V$ARCHIVED_LOG "
      + " WHERE FIRST_CHANGE# = (SELECT MAX(FIRST_CHANGE#) FROM V$ARCHIVED_LOG WHERE STATUS = 'A' AND "
      + "                        DICTIONARY_END = 'YES' AND FIRST_CHANGE# <= :scn) ";
  private static final String SELECT_DICTIONARY_LOGS_QUERY =
      " SELECT NAME, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, DICTIONARY_BEGIN, "
      + "    DICTIONARY_END "
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
      + "     DICTIONARY_END "
      + " FROM V$ARCHIVED_LOG "
      + " WHERE STATUS = 'A' AND FIRST_TIME <= :time AND NEXT_TIME > :time "
      + " UNION "
      + " SELECT VLOGFILE.MEMBER, THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE#, "
      + "     'NO' AS DICTIONARY_BEGIN, 'NO' AS DICTIONARY_END  "
      + " FROM V$LOG, "
      + "     (SELECT GROUP#, MEMBER, ROW_NUMBER() OVER (PARTITION BY GROUP# ORDER BY GROUP#) AS ROWNO "
      + "      FROM V$LOGFILE) VLOGFILE "
      + " WHERE V$LOG.GROUP# = VLOGFILE.GROUP# AND V$LOG.MEMBERS = VLOGFILE.ROWNO AND V$LOG.ARCHIVED = 'NO' AND "
      + "     FIRST_TIME <= :time AND (NEXT_TIME > :time OR NEXT_TIME IS NULL) ";
  private static final String SELECT_LOGS_FROM_DATE_ARG = ":time";

  // Query retrieving the current redo log files registered in the current LogMiner session.
  private final String SELECT_LOGMNR_LOGS_QUERY = "SELECT FILENAME FROM V$LOGMNR_LOGS ORDER BY LOW_SCN";

  private final Connection connection;
  private final String configOptions;
  private final boolean continuousMine;
  private final boolean dictionaryFromRedoLogs;
  private final boolean ddlDictTracking;
  private final DateTimeFormatter dateTimeFormatter;
  private final List<RedoLog> currentLogList;
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


    public Builder(Connection connection) {
      this.connection = Preconditions.checkNotNull(connection);
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

    public LogMinerSession build() {
      List<String> configOptions = new ArrayList<>();
      configOptions.add(NO_SQL_DELIMITER_OPTION);
      configOptions.add("DBMS_LOGMNR." + dictionarySource.name());

      if (continuousMine) {
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

      return new LogMinerSession(connection, configOptions);
    }

  }

  private LogMinerSession(Connection connection, List<String> configOptions) {
    this.connection = connection;
    this.configOptions = String.join(" + ", configOptions);
    continuousMine = configOptions.contains(CONTINUOUS_MINE_OPTION);
    dictionaryFromRedoLogs = configOptions.contains(DICT_FROM_REDO_LOGS_OPTION);
    ddlDictTracking = configOptions.contains(DDL_DICT_TRACKING_OPTION);
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
      BigDecimal start = logList.get(0).getNextChange();
      start(start, scn);  // Force the dictionary extraction by starting a LogMiner session with a proper range.
    } else {
      BigDecimal start = logList.get(logList.size() - 1).getNextChange();
      updateLogList(start, scn);
    }
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
            rs.getBoolean(9)
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

    if (newLogList.size() == 0 || newLogList.get(0).getFirstChange().compareTo(start) > 1) {
      throw new StageException(JdbcErrors.JDBC_600, start, end);
    }

    Set<String> current = currentLogList.stream().map(log -> log.getPath()).collect(Collectors.toSet());
    Set<String> next = newLogList.stream().map(log -> log.getPath()).collect(Collectors.toSet());

    // Remove current active logs and logs no longer needed from the LogMiner internal list (V$LOGMNR_LOGS). We must do
    // it before registering the new redo logs, because if a registered online log has been archived it will
    // appear in the new list, and trying to add it to LogMiner would result in an ORA-01289 error
    // ("duplicate redo logfile"). Current active logs are again registered after this.
    for (RedoLog log : currentLogList) {
      if (!next.contains(log.getPath()) || log.isCurrentLog()) {
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
      if (current.contains(log.getPath()) && !log.isCurrentLog()) {
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
            rs.getBoolean(9)
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
            rs.getBoolean(9)
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
   * Returns the list of redo logs that covers a given range of changes in database.
   *
   * @param start The initial SCN to cover.
   * @param end The last SCN to cover.
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
            rs.getBoolean(9)
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
