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
import org.parboiled.common.Preconditions;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Objects;

public class RedoLog {
  private final String path;
  private final BigDecimal group;
  private final BigDecimal thread;
  private final BigDecimal sequence;
  private final LocalDateTime firstTime;
  private final LocalDateTime nextTime;
  private final BigDecimal firstChange;
  private final BigDecimal nextChange;
  private final boolean dictionaryBegin;
  private final boolean dictionaryEnd;
  private final String status;
  private final boolean onlineLog;
  private final boolean archived;

  public RedoLog(
      String path,
      BigDecimal group,
      BigDecimal thread,
      BigDecimal sequence,
      LocalDateTime firstTime,
      LocalDateTime nextTime,
      BigDecimal firstChange,
      BigDecimal nextChange,
      boolean dictionaryBegin,
      boolean dictionaryEnd,
      String status,
      boolean onlineLog,
      boolean archived
  ) {
    this.path = Preconditions.checkNotNull(path);
    this.group = group;
    this.thread = Preconditions.checkNotNull(thread);
    this.sequence = Preconditions.checkNotNull(sequence);
    this.firstTime = Preconditions.checkNotNull(firstTime);
    this.nextTime = Preconditions.checkNotNull(nextTime);
    this.firstChange = Preconditions.checkNotNull(firstChange);
    this.nextChange = Preconditions.checkNotNull(nextChange);
    this.dictionaryBegin = dictionaryBegin;
    this.dictionaryEnd = dictionaryEnd;
    this.status = Preconditions.checkNotNull(status).toUpperCase();
    this.onlineLog = onlineLog;
    this.archived = archived;
  }

  public RedoLog(
      String path,
      BigDecimal group,
      BigDecimal thread,
      BigDecimal sequence,
      Timestamp firstTime,
      Timestamp nextTime,
      BigDecimal firstChange,
      BigDecimal nextChange,
      boolean dictionaryBegin,
      boolean dictionaryEnd,
      String status,
      boolean onlineLog,
      boolean archived
  ) {
    this(path,
        group,
        thread,
        sequence,
        firstTime == null ? null : firstTime.toLocalDateTime(),
        nextTime == null ? null : nextTime.toLocalDateTime(),
        firstChange,
        nextChange,
        dictionaryBegin,
        dictionaryEnd,
        status,
        onlineLog,
        archived);
  }

  public boolean hasSameData(RedoLog log) {
    return thread.equals(log.getThread()) && sequence.equals(log.getSequence());
  }

  @Override
  public String toString() {
    String dict;
    if (dictionaryBegin && dictionaryEnd) {
      dict = "complete";
    } else if (dictionaryBegin) {
      dict = "begin";
    } else if (dictionaryEnd) {
      dict = "end";
    } else {
      dict = "no";
    }
    return Utils.format(
        "{}, seq: {}/{}, start: {} ({}), end: {} ({}), status: {}, online: {}, archived: {}, dictionary: {}",
        path, thread, sequence, firstChange, firstTime, nextChange, nextTime, status, onlineLog, archived, dict
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RedoLog log = (RedoLog) o;
    return dictionaryBegin == log.dictionaryBegin &&
        dictionaryEnd == log.dictionaryEnd &&
        onlineLog == log.onlineLog &&
        archived == log.archived &&
        path.equals(log.path) &&
        Objects.equals(group, log.group) &&
        thread.equals(log.thread) &&
        sequence.equals(log.sequence) &&
        firstTime.equals(log.firstTime) &&
        Objects.equals(nextTime, log.nextTime) &&
        firstChange.equals(log.firstChange) &&
        nextChange.equals(log.nextChange) &&
        status.equals(log.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        path,
        group,
        thread,
        sequence,
        firstTime,
        nextTime,
        firstChange,
        nextChange,
        dictionaryBegin,
        dictionaryEnd,
        status,
        onlineLog,
        archived
    );
  }

  public String getPath() {
    return path;
  }

  public BigDecimal getGroup() {
    return group;
  }

  public BigDecimal getThread() {
    return thread;
  }

  public BigDecimal getSequence() {
    return sequence;
  }

  public LocalDateTime getFirstTime() {
    return firstTime;
  }

  public LocalDateTime getNextTime() {
    return nextTime;
  }

  public BigDecimal getFirstChange() {
    return firstChange;
  }

  public BigDecimal getNextChange() {
    return nextChange;
  }

  public boolean isDictionaryBegin() {
    return dictionaryBegin;
  }

  public boolean isDictionaryEnd() {
    return dictionaryEnd;
  }

  public String getStatus() {
    return status;
  }

  public boolean isOnlineLog() {
    return onlineLog;
  }

  /**
   * For redo logs in V$ARCHIVED_LOG this is always True. For redo logs in V$LOG (i.e. the online redo logs), this can
   * be either True or False: it is always False for the current redo log (i.e. the one that the database is
   * currently writing), and is False for any other online redo log still pending to archive.
   */
  public boolean isArchived() {
    return archived;
  }

}
