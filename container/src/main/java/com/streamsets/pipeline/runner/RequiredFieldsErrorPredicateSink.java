/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.container.ErrorMessage;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.util.ContainerError;

import java.util.ArrayList;
import java.util.List;

public class RequiredFieldsErrorPredicateSink implements FilterRecordBatch.Predicate, FilterRecordBatch.Sink {

  private final List<String> requiredFields;
  private final String instanceName;
  private final ErrorSink errorSink;
  private final List<String> missingFields;
  private int counter;

  public RequiredFieldsErrorPredicateSink(String instanceName, List<String> requiredFields, ErrorSink errorSink) {
    this.requiredFields = requiredFields;
    this.instanceName = instanceName;
    this.errorSink = errorSink;
    missingFields = new ArrayList<>();
  }

  @Override
  public void add(Record record, ErrorMessage reason) {
    RecordImpl recordImpl = (RecordImpl) record;
    recordImpl.getHeader().setErrorId(reason.getId().toString());
    recordImpl.getHeader().setErrorMessage(reason);
    errorSink.addRecord(instanceName, recordImpl);
    counter++;
  }

  public int size() {
    return counter;
  }

  @Override
  public boolean evaluate(Record record) {
    boolean eval = true;
    if (requiredFields != null) {
      missingFields.clear();
      for (String field : requiredFields) {
        if (!record.has(field)) {
          missingFields.add(field);
        }
      }
      eval = missingFields.isEmpty();
    }
    return eval;
  }

  @Override
  public ErrorMessage getRejectedMessage() {
    Preconditions.checkState(!missingFields.isEmpty(), "Called for record that passed the predicate check");
    return new ErrorMessage(ContainerError.CONTAINER_0050, instanceName, missingFields);
  }

  @Override
  public String toString() {
    return Utils.format("RequiredFieldsPredicateSink[fields='{}' size='{}']", requiredFields, size());
  }

}
