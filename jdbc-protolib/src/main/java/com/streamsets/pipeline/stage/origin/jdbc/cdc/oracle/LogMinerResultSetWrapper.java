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

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * ResultSet wrapper for a V$LOGMNR_CONTENT query. It can iterate over the query results and return
 * {@link LogMinerRecord} objects for an easier access to the retrieved data.
 */
public class LogMinerResultSetWrapper {
  private static final String SCN_COLUMN = "SCN";
  private static final String USERNAME_COLUMN = "USERNAME";
  private static final String OPERATION_CODE_COLUMN = "OPERATION_CODE";
  private static final String TIMESTAMP_COLUMN = "TIMESTAMP";
  private static final String SQL_REDO_COLUMN = "SQL_REDO";
  private static final String TABLE_NAME_COLUMN = "TABLE_NAME";
  private static final String COMMIT_SCN_COLUMN = "COMMIT_SCN";
  private static final String COMMIT_SCN_COLUMN_OLD = "CSCN";
  private static final String SEQUENCE_COLUMN = "SEQUENCE#";
  private static final String CSF_COLUMN = "CSF";
  private static final String XIDUSN_COLUMN = "XIDUSN";
  private static final String XIDSLT_COLUMN = "XIDSLT";
  private static final String XIDSQN_COLUMN = "XIDSQN";
  private static final String RS_ID_COLUMN = "RS_ID";
  private static final String SSN_COLUMN = "SSN";
  private static final String SEG_OWNER_COLUMN = "SEG_OWNER";
  private static final String ROLLBACK_COLUMN = "ROLLBACK";
  private static final String ROW_ID_COLUMN = "ROW_ID";
  private static final String REDO_VALUE = "REDO_VALUE";
  private static final String UNDO_VALUE = "UNDO_VALUE";

  private ResultSet resultSet;
  private int databaseVersion;

  public LogMinerResultSetWrapper(ResultSet rs, int databaseVersion) {
    this.databaseVersion = databaseVersion;
    resultSet = rs;
  }

  /**
   * Move the database cursor to the next position.
   *
   * This method can block and wait for future data. This happens when CONTINUOUS_MINE is enabled in
   * {@link LogMinerSession} and the mining window specified in {@link LogMinerSession#start} spans across the future.
   * In this case, when the database cursor reaches the current database time, calling this method will block for any
   * future transaction written into the redo logs, or until the {@code end} time limit defined in
   * {@link LogMinerSession#start} is reached. When the latter, this method will return False.
   *
   * When CONTINUOUS_MINE is not enabled, this method is non-blocking and always return False when the cursor
   * traversed all the current transactions found for that mining windows in the redo logs, irrespective of the mining
   * window span across the future.
   */
  public boolean next() throws SQLException {
    return resultSet.next();
  }

  public void close() throws SQLException {
    resultSet.close();
  }

  public boolean isClosed() throws SQLException {
    return resultSet.isClosed();
  }

  public void setFetchSize(int rows) throws SQLException {
    resultSet.setFetchSize(rows);
  }

  public LogMinerRecord getRecord() throws SQLException {
    return new LogMinerRecord(
        resultSet.getBigDecimal(SCN_COLUMN),
        resultSet.getString(USERNAME_COLUMN),
        resultSet.getShort(OPERATION_CODE_COLUMN),
        resultSet.getString(TIMESTAMP_COLUMN),
        resultSet.getString(SQL_REDO_COLUMN),
        resultSet.getString(TABLE_NAME_COLUMN),
        resultSet.getBigDecimal(databaseVersion >= 11 ? COMMIT_SCN_COLUMN : COMMIT_SCN_COLUMN_OLD),
        resultSet.getInt(SEQUENCE_COLUMN),
        resultSet.getLong(XIDUSN_COLUMN),
        resultSet.getString(XIDSLT_COLUMN),
        resultSet.getString(XIDSQN_COLUMN),
        resultSet.getString(RS_ID_COLUMN),
        resultSet.getObject(SSN_COLUMN),
        resultSet.getString(SEG_OWNER_COLUMN),
        resultSet.getInt(ROLLBACK_COLUMN),
        resultSet.getString(ROW_ID_COLUMN),
        resultSet.getInt(CSF_COLUMN) != 1,
        resultSet.getBigDecimal(REDO_VALUE),
        resultSet.getBigDecimal(UNDO_VALUE),
        resultSet.getTimestamp(TIMESTAMP_COLUMN)
    );
  }
}
