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
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;


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

  // Templates to build LogMiner commands
  private static final String START_TIME_ARG = "STARTTIME => TO_DATE(?, 'DD-MM-YYYY HH24:MI:SS')";
  private static final String END_TIME_ARG = "ENDTIME => TO_DATE(?, 'DD-MM-YYYY HH24:MI:SS')";
  private static final String START_SCN_ARG = "STARTSCN => ?";
  private static final String END_SCN_ARG = "ENDSCN => ?";
  private static final String STOP_LOGMNR_CMD = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;";
  private static final String START_LOGMNR_CMD = "BEGIN"
      + " DBMS_LOGMNR.START_LOGMNR("
      + "   {}," // Placeholder for START_TIME_ARG or START_SCN_ARG
      + "   {}," // Placeholder for END_TIME_ARG or END_SCN_ARG
      + "   OPTIONS => {}" // Option flags
      + " );"
      + " END;";
  private static final String DATETIME_FORMAT = "dd-MM-yyyy HH:mm:ss";  // Must match START_TIME_ARG and END_TIME_ARG.

  private final Connection connection;
  private final String configOptions;
  private final DateTimeFormatter dateTimeFormatter;

  /**
   * LogMinerSession builder. It allows configuring optional parameters for the LogMiner session. Actual LogMiner
   * session is eventually created through the {@link LogMinerSession#start} functions.
   */
  public static class Builder {
    private final Connection connection;
    private DictionaryValues dictionarySource = DictionaryValues.DICT_FROM_ONLINE_CATALOG;
    private boolean committedDataOnly;
    private boolean ddlDictTracking;

    public Builder(Connection connection) {
      this.connection = Preconditions.checkNotNull(connection);
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
      configOptions.add(CONTINUOUS_MINE_OPTION);
      configOptions.add(NO_SQL_DELIMITER_OPTION);
      configOptions.add("DBMS_LOGMNR." + this.dictionarySource.name());

      if (committedDataOnly) {
        configOptions.add(COMMITTED_DATA_ONLY_OPTION);
      }
      if (ddlDictTracking) {
        configOptions.add(DDL_DICT_TRACKING_OPTION);
      }

      return new LogMinerSession(connection, configOptions);
    }

  }

  private LogMinerSession(Connection connection, List<String> configOptions) {
    this.connection = connection;
    this.configOptions = String.join(" + ", configOptions);
    dateTimeFormatter = new DateTimeFormatterBuilder()
        .parseLenient()
        .appendPattern(DATETIME_FORMAT)
        .toFormatter();
  }

  /**
   * Starts a new LogMiner session. This closes the currently active session, if any.
   * The {@code start} and {@code end} parameters define the range of redo records accessible in this session for
   * minning.
   *
   * @param start Available redo records will have a timestamp greater than or equal to {@code start}.
   * @param end Available redo records will have a timestamp less than or equal to {@code end}.
   * @throws SQLException If an invalid SCN was configured, no CDC change is available for the configured range,
   * database is unavailable. See Table 90-16 in https://docs.oracle.com/database/121/ARPLS/d_logmnr.htm#ARPLS022 for a
   * complete list of causes.
   */
  public void start(LocalDateTime start, LocalDateTime end) throws SQLException {
    LOG.info("Starting LogMiner session with window ({}, {}).", start, end);
    String command = Utils.format(START_LOGMNR_CMD, START_TIME_ARG, END_TIME_ARG, this.configOptions);

    try (PreparedStatement statement = connection.prepareStatement(command)) {
      statement.setString(1, start.format(dateTimeFormatter));
      statement.setString(2, end.format(dateTimeFormatter));
      statement.execute();
    } catch (SQLException ex) {
      LOG.error("Could not start LogMiner session with window ({}, {}): {}", start, end, ex);
      throw ex;
    }
  }

  /**
   * Starts a new LogMiner session. This closes the currently active session, if any.
   * The {@code start} and {@code end} parameters define the range of redo records accessible in this session for
   * minning.
   *
   * @param start Available redo records will have a SCN greater than or equal to {@code start}.
   * @param end Available redo records will have a SCN less than or equal to {@code end}.
   * @throws SQLException If an invalid SCN was configured, no CDC change is available for the configured range,
   * database is unavailable. See Table 90-16 in https://docs.oracle.com/database/121/ARPLS/d_logmnr.htm#ARPLS022 for a
   * complete list of causes.
   */
  public void start(BigDecimal start, BigDecimal end) throws SQLException {
    LOG.info("Starting LogMiner session with window ({}, {}).", start, end);
    String command = Utils.format(START_LOGMNR_CMD, START_SCN_ARG, END_SCN_ARG, this.configOptions);

    try (PreparedStatement statement = connection.prepareStatement(command)) {
      statement.setBigDecimal(1, start);
      statement.setBigDecimal(2, end);
      statement.execute();
    } catch (SQLException ex) {
      LOG.error("Could not start LogMiner session with window ({}, {}): {}", start, end, ex);
      throw ex;
    }
  }

  /**
   * Starts a new LogMiner session. This closes the currently active session, if any.
   * The {@code start} and {@code end} parameters define the range of redo records accessible in this session for
   * minning.
   *
   * @param start Available redo records will have a timestamp greater than or equal to {@code start}.
   * @param end Available redo records will have a SCN less than or equal to {@code end}.
   * @throws SQLException If an invalid SCN was configured, no CDC change is available for the configured range,
   * database is unavailable. See Table 90-16 in https://docs.oracle.com/database/121/ARPLS/d_logmnr.htm#ARPLS022 for a
   * complete list of causes.
   */
  public void start(LocalDateTime start, BigDecimal end) throws SQLException {
    LOG.info("Starting LogMiner session with window ({}, {}).", start, end);
    String command = Utils.format(START_LOGMNR_CMD, START_TIME_ARG, END_SCN_ARG, this.configOptions);

    try (PreparedStatement statement = connection.prepareStatement(command)) {
      statement.setString(1, start.format(dateTimeFormatter));
      statement.setBigDecimal(2, end);
      statement.execute();
    } catch (SQLException ex) {
      LOG.error("Could not start LogMiner session with window ({}, {}): {}", start, end, ex);
      throw ex;
    }
  }

  /**
   * Starts a new LogMiner session. This closes the currently active session, if any.
   * The {@code start} and {@code end} parameters define the range of redo records accessible in this session for
   * minning.
   *
   * @param start Available redo records will have a SCN greater than or equal to {@code start}.
   * @param end Available redo records will have a timestamp less than or equal to {@code end}.
   * @throws SQLException If an invalid SCN was configured, no CDC change is available for the configured range,
   * database is unavailable. See Table 90-16 in https://docs.oracle.com/database/121/ARPLS/d_logmnr.htm#ARPLS022 for a
   * complete list of causes.
   */
  public void start(BigDecimal start, LocalDateTime end) throws SQLException {
    LOG.info("Starting LogMiner session with window ({}, {}).", start, end);
    String command =  Utils.format(START_LOGMNR_CMD, START_SCN_ARG, END_TIME_ARG, this.configOptions);

    try (PreparedStatement statement = connection.prepareStatement(command)) {
      statement.setBigDecimal(1, start);
      statement.setString(2, end.format(dateTimeFormatter));
      statement.execute();
    } catch (SQLException ex) {
      LOG.error("Could not start LogMiner session with window ({}, {}): {}", start, end, ex);
      throw ex;
    }
  }

  /**
   * Closes the current LogMiner session.
   *
   * @throws SQLException If no LogMiner session is currently active, the database connection is closed, or a
   * database error occurred.
   */
  public void close() throws SQLException {
    LOG.info("Explicitly closing current LogMiner session.");
    try (Statement statement = connection.createStatement()) {
      statement.execute(STOP_LOGMNR_CMD);
    } catch (SQLException ex) {
      LOG.error("Could not close LogMiner session: {}", ex);
      throw ex;
    }
  }

}
