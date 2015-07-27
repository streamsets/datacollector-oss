/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;

import java.util.ArrayList;
import java.util.List;

public class RequiredFieldsPredicate implements FilterRecordBatch.Predicate {
  private final List<String> requiredFields;
  private final List<String> missingFields;

  public RequiredFieldsPredicate(List<String> requiredFields) {
    this.requiredFields = requiredFields;
    missingFields = new ArrayList<>();
  }

  @Override
  public boolean evaluate(Record record) {
    boolean eval = true;
    if (requiredFields != null && !requiredFields.isEmpty()) {
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
    return new ErrorMessage(ContainerError.CONTAINER_0050, missingFields);
  }

}
