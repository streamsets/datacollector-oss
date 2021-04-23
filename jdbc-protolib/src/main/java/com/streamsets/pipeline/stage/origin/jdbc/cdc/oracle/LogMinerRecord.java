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

import com.streamsets.pipeline.api.impl.Utils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class LogMinerRecord {
  private final BigDecimal scn;
  private final String userName;
  private final short operationCode;
  private final String timestamp;
  private final LocalDateTime localDateTime;
  private final String sqlRedo;
  private final String tableName;
  private final BigDecimal commitSCN;
  private final int sequence;
  private final long xidusn;
  private final String xidslt;
  private final String xidsqn;
  private final String rsId;
  private final Object ssn;
  private final String segOwner;
  private final int rollback;
  private final String rowId;
  private final boolean endOfRecord;
  private final BigDecimal redoValue;
  private final BigDecimal undoValue;
  private final Timestamp precisionTimestamp;

  public LogMinerRecord(
      BigDecimal scn,
      String userName,
      short operationCode,
      String timestamp,
      String sqlRedo,
      String tableName,
      BigDecimal commitSCN,
      int sequence,
      long xidusn,
      String xidslt,
      String xidsqn,
      String rsId,
      Object ssn,
      String segOwner,
      int rollback,
      String rowId,
      boolean endOfRecord,
      BigDecimal redoValue,
      BigDecimal undoValue,
      Timestamp precisionTimestamp
  ) {
    this.scn = scn;
    this.userName = userName;
    this.operationCode = operationCode;
    this.timestamp = timestamp;
    this.localDateTime = Timestamp.valueOf(timestamp).toLocalDateTime();
    this.sqlRedo = sqlRedo == null ? "" : sqlRedo;
    this.tableName = tableName;
    this.commitSCN = commitSCN;
    this.sequence = sequence;
    this.xidusn = xidusn;
    this.xidslt = xidslt;
    this.xidsqn = xidsqn;
    this.rsId = rsId;
    this.ssn = ssn;
    this.segOwner = segOwner;
    this.rollback = rollback;
    this.rowId = rowId;
    this.endOfRecord = endOfRecord;
    this.redoValue = redoValue;
    this.undoValue = undoValue;
    this.precisionTimestamp = precisionTimestamp;
  }

  public BigDecimal getScn() {
    return scn;
  }

  public String getUserName() {
    return userName;
  }

  public short getOperationCode() {
    return operationCode;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public LocalDateTime getLocalDateTime() {
    return localDateTime;
  }

  public String getSqlRedo() {
    return sqlRedo;
  }

  public String getTableName() {
    return tableName;
  }

  public BigDecimal getCommitSCN() {
    return commitSCN;
  }

  public int getSequence() {
    return sequence;
  }

  public long getXIDUSN() {
    return xidusn;
  }

  public String getXIDSLT() {
    return xidslt;
  }

  public String getXIDSQN() {
    return xidsqn;
  }

  public String getXID() {
    return Utils.format("{}.{}.{}", getXIDUSN(), getXIDSLT(), getXIDSQN());
  }

  public String getRsId() {
    return rsId;
  }

  public Object getSsn() {
    return ssn;
  }

  public String getSegOwner() {
    return segOwner;
  }

  public int getRollback() {
    return rollback;
  }

  public String getRowId() {
    return rowId;
  }

  public boolean isEndOfRecord() {
    return endOfRecord;
  }

  public BigDecimal getRedoValue() { return redoValue; }

  public BigDecimal getUndoValue() { return undoValue; }

  public Timestamp getPrecisionTimestamp() { return precisionTimestamp; }

  @Override
  public String toString() {
    return "LogMinerRecord "
        + "{"
        + "rollback=" + rollback + " - "
        + "endOfRecord=" + endOfRecord + " - "
        + "xidusn=" + xidusn + " - "
        + "xidslt=" + xidslt + " - "
        + "xidsqn=" + xidsqn + " - "
        + "scn=" + scn + " - "
        + "commitSCN=" + commitSCN + " - "
        + "sequence=" + sequence + " - "
        + "ssn=" + ssn + " - "
        + "rsId=" + rsId + " - "
        + "operationCode=" + operationCode + " - "
        + "rowId=" + rowId + " - "
        + "timestamp=" + timestamp + " - "
        + "localDateTime=" + localDateTime + " - "
        + "segOwner=" + segOwner + " - "
        + "tableName=" + tableName + " - "
        + "userName=" + userName + " - "
        + "sqlRedo=" + sqlRedo + " - "
        + "redoValue=" + redoValue + " - "
        + "undoValue=" + undoValue + " - "
        + "precisionTimestamp=" + precisionTimestamp + " - "
        + "}";
  }
}
