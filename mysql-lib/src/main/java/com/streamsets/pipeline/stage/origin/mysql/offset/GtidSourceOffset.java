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
package com.streamsets.pipeline.stage.origin.mysql.offset;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GtidSourceOffset implements SourceOffset {
  private static final ObjectMapper mapper = new ObjectMapper();

  @JsonProperty
  private final String gtidSet;

  @JsonProperty
  private final Map<String, Long> incompleteTransactions;

  // gtid of last event
  @JsonIgnore
  private transient String gtid;

  // seqno of last event
  @JsonIgnore
  private transient long seqNo;

  public GtidSourceOffset(String gtidSet) {
    this(gtidSet, Collections.<String, Long>emptyMap());
  }

  @JsonCreator
  public GtidSourceOffset(
      @JsonProperty("gtidSet") String gtidSet,
      @JsonProperty("incompleteTransactions") Map<String, Long> incompleteTransactions
  ) {
    this.gtidSet = gtidSet;
    this.incompleteTransactions = incompleteTransactions;
  }

  @Override
  public String format() {
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void positionClient(BinaryLogClient client) {
    client.setGtidSet(gtidSet);
  }

  public GtidSourceOffset withIncompleteTransaction(String gtid, long seqNo) {
    GtidSourceOffset off = new GtidSourceOffset(gtidSet, new HashMap<>(incompleteTransactions));
    off.incompleteTransactions.put(gtid, seqNo);
    off.gtid = gtid;
    off.seqNo = seqNo;
    return off;
  }

  public GtidSourceOffset finishTransaction(String gtid) {
    GtidSourceOffset off = new GtidSourceOffset(gtidSet, new HashMap<>(incompleteTransactions));
    off.gtid = gtid;
    off.seqNo = seqNo;
    off.incompleteTransactions.remove(gtid);
    return off;
  }

  public GtidSourceOffset withGtidSet(String gtidSet) {
    GtidSourceOffset off = new GtidSourceOffset(gtidSet, incompleteTransactions);
    off.gtid = gtid;
    off.seqNo = seqNo;
    return off;
  }

  @JsonIgnore
  public String getGtid() {
    return gtid;
  }

  @JsonIgnore
  public long getSeqNo() {
    return seqNo;
  }

  /**
   * Check if given gtid + seqNo pair is contained in this incomplete transactions.
   * @param gtid
   * @param seqNo
   * @return true if incomplete transactions have given gtid and
   * its associated seqNo is greater or equal to given seqNo.
   */
  public boolean incompleteTransactionsContain(String gtid, long seqNo) {
    Long s = incompleteTransactions.get(gtid);
    return s != null && s >= seqNo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GtidSourceOffset that = (GtidSourceOffset) o;

    if (gtidSet != null ? !gtidSet.equals(that.gtidSet) : that.gtidSet != null) {
      return false;
    }
    return incompleteTransactions != null
        ? incompleteTransactions.equals(that.incompleteTransactions)
        : that.incompleteTransactions == null;

  }

  @Override
  public int hashCode() {
    int result = gtidSet != null ? gtidSet.hashCode() : 0;
    result = 31 * result + (incompleteTransactions != null ? incompleteTransactions.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return format();
  }

  public static GtidSourceOffset parse(String offset) {
    // offset can be either json object or simple gtid-set
    if (offset.trim().startsWith("{")) {
      try {
        return mapper.readValue(offset, GtidSourceOffset.class);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    } else {
      // simple gtid-set
      return new GtidSourceOffset(offset);
    }
  }
}
