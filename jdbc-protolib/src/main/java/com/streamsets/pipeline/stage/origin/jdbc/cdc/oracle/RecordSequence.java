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

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

class RecordSequence implements Serializable {
  final Map<String, String> headers;
  final String sqlString;
  final int seq;
  final int opCode;
  final String rsId;
  final Object ssn;
  final String rowId;
  final LocalDateTime timestamp;

  RecordSequence(Map<String, String> headers, String sql, int seq, int opCode, String rsId, Object ssn,
      String rowId, LocalDateTime timestamp) {
    this.headers = headers;
    this.sqlString = sql;
    this.seq = seq;
    this.opCode = opCode;
    this.rsId = rsId;
    this.ssn = ssn;
    this.rowId = rowId;
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof RecordSequence))
      return false;
    RecordSequence recordSequence = (RecordSequence) o;
    if (this.rsId.equals(recordSequence.rsId) &&
        this.ssn.equals(recordSequence.ssn)) {
      if (this.rowId == null || recordSequence.rowId == null)
        return false;
      else
        return this.rowId.equals(recordSequence.rowId);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return rsId.hashCode() + ssn.hashCode();
  }

  @Override
  public String toString() {
    return "RecordSequence "
        + "{"
        + "headers=" + StringUtils.join(headers, " <> ") + " - "
        + "sqlString=" + sqlString  + " - "
        + "seq=" + seq  + " - "
        + "opCode=" + opCode  + " - "
        + "rsId=" + rsId  + " - "
        + "ssn=" + ssn  + " - "
        + "rowId=" + rowId  + " - "
        + "timestamp=" + timestamp  + " - "
        + '}';
  }
}
