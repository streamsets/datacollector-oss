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
package com.streamsets.pipeline.lib.parser;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Record;

/**
 * Thrown in case when the record can't be parsed, but the problem is not severe enough to invalidate the whole stream,
 * e.g. the parsing can continue beyond the un-parsable record.
 */
public class RecoverableDataParserException extends DataParserException {

  /**
   * This is the partial record containing whole information that human being will need to re-construct the original record
   * and correct it.
   */
  private final Record unparsedRecord;

  public RecoverableDataParserException(Record unparsedRecord, ErrorCode errorCode, Object... params) {
    super(errorCode, params);
    this.unparsedRecord = unparsedRecord;
  }

  public Record getUnparsedRecord() {
    return unparsedRecord;
  }
}
