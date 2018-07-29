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
