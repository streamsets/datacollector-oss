/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.runner;

import com.google.common.collect.AbstractIterator;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;

import java.util.Iterator;

public class FilterRecordBatch implements Batch {
  private final Batch batch;
  private final Predicate[] predicates;
  private final Sink filteredOutRecordsSink;

  public interface Predicate {

    public boolean evaluate(Record record);

    public ErrorMessage getRejectedMessage();

  }

  public interface Sink {

    public void add(Record record, ErrorMessage message);

  }

  public FilterRecordBatch(Batch batch, Predicate[] predicates, Sink filteredOutRecordsSink) {
    this.batch = batch;
    this.predicates = predicates;
    this.filteredOutRecordsSink = filteredOutRecordsSink;
  }

  @Override
  public String getSourceOffset() {
    return batch.getSourceOffset();
  }

  @Override
  public Iterator<Record> getRecords() {
    return new RecordIterator(batch.getRecords());
  }

  private class RecordIterator extends AbstractIterator<Record> {
    private Iterator<Record> iterator;

    public RecordIterator(Iterator<Record> iterator) {
      this.iterator = iterator;
    }

    @Override
    protected Record computeNext() {
      Record next = null;
      while (next == null && iterator.hasNext()) {
        boolean passed = true;
        ErrorMessage rejectedMessage = null;
        Record record = iterator.next();
        for (Predicate predicate : predicates) {
          passed = predicate.evaluate(record);
          if (!passed) {
            rejectedMessage = predicate.getRejectedMessage();
            break;
          }
        }
        if (passed) {
          next = record;
        } else {
          filteredOutRecordsSink.add(record, rejectedMessage);
        }
      }
      if (next == null && !iterator.hasNext()) {
        endOfData();
      }
      return next;
    }
  }

}
