/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;

import java.util.Collections;
import java.util.List;

public class ParseResult {
  private static final List<Record> EMPTY_LIST = Collections.emptyList();
  private final List<Record> records;
  private final OnRecordErrorException onRecordErrorException;

  public ParseResult(OnRecordErrorException onRecordErrorException) {
    this(EMPTY_LIST, onRecordErrorException);
  }

  public ParseResult(List<Record> records) {
    this(records, null);
  }

  private ParseResult(List<Record> records, OnRecordErrorException onRecordErrorException) {
    this.records = records;
    this.onRecordErrorException = onRecordErrorException;
  }

  public List<Record> getRecords() throws OnRecordErrorException {
    if (onRecordErrorException != null) {
      throw onRecordErrorException;
    }
    return records;
  }
}
