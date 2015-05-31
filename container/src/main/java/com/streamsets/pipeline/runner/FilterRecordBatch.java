/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.google.common.collect.AbstractIterator;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;

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
