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
