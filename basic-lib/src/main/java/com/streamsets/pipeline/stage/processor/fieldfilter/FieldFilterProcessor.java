/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldfilter;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FieldFilterProcessor extends SingleLaneRecordProcessor {

  private final FilterOperation filterOperation;
  private final List<String> fields;


  public FieldFilterProcessor(FilterOperation filterOperation, List<String> fields) {
    this.filterOperation = filterOperation;
    this.fields = fields;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> fieldsToRemove = new HashSet<>();
    if(filterOperation == FilterOperation.REMOVE) {
      fieldsToRemove.addAll(fields);
    } else {
      fieldsToRemove.addAll(record.getFieldPaths());
      fieldsToRemove.removeAll(fields);
      fieldsToRemove.remove("");
    }
    for (String fieldToRemove : fieldsToRemove) {
      record.delete(fieldToRemove);
    }
    batchMaker.addRecord(record);
  }

}