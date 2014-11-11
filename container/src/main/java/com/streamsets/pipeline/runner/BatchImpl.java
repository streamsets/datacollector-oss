/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.runner;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BatchImpl implements Batch {
  private final String stageInstance;
  private final List<String> requiredFields;
  private final List<Record> records;
  private final String sourceOffset;
  private final ErrorRecordSink errorSink;
  private boolean got;

  public BatchImpl(String stageInstance, SourceOffsetTracker offsetTracker, List<String> requiredFields,
      List<Record> records, ErrorRecordSink errorSink) {
    this.stageInstance = stageInstance;
    this.requiredFields = requiredFields;
    this.records = records;
    sourceOffset = offsetTracker.getOffset();
    this.errorSink = errorSink;
    got = false;
  }

  @Override
  public String getSourceOffset() {
    return sourceOffset;
  }

  @Override
  public Iterator<Record> getRecords() {
    Preconditions.checkState(!got, "The record iterator can be obtained only once");
    got = true;
    Iterator<Record> iterator = records.iterator();
    if (requiredFields != null) {
      iterator = new RecordIterator(records.iterator());
    } else {
      iterator = Iterators.unmodifiableIterator(iterator);
    }
    return iterator;
  }

  public int getSize() {
    return records.size();
  }

  class RecordIterator extends AbstractIterator<Record> {
    private Iterator<Record> iterator;

    public RecordIterator(Iterator<Record> iterator) {
      this.iterator = iterator;
    }

    @Override
    protected Record computeNext() {
      Record next = null;
      while (next == null && iterator.hasNext()) {
        Record record = iterator.next();
        List<String> missingFields = new ArrayList<String>();
        for (String field : requiredFields) {
          if (!record.hasField(field)) {
            missingFields.add(field);
          }
        }
        if (missingFields.isEmpty()) {
          next = record;
        } else {
          errorSink.addRecord(stageInstance, record, ErrorRecord.ERROR.REQUIRED_FIELDS_MISSING, missingFields);
        }
      }
      if (next == null && !iterator.hasNext()) {
        endOfData();
      }
      return next;
    }
  }

}
