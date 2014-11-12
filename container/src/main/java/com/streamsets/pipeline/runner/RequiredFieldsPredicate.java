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

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.validation.Issue;

import java.util.ArrayList;
import java.util.List;

public class RequiredFieldsPredicate implements FilterRecordBatch.Predicate {
  private static final String MISSING_REQUIRED_FIELDS_KEY = "missing.required.fields";
  private static final String MISSING_REQUIRED_FIELDS_DEFAULT = "The record misses the following required fields {}";

  private final List<String> requiredFields;
  private final List<String> missingFields;

  public RequiredFieldsPredicate(List<String> requiredFields) {
    this.requiredFields = requiredFields;
    missingFields = new ArrayList<String>();
  }

  @Override
  public boolean evaluate(Record record) {
    boolean eval = true;
    if (requiredFields != null) {
      missingFields.clear();
      for (String field : requiredFields) {
        if (!record.hasField(field)) {
          missingFields.add(field);
        }
      }
      eval = missingFields.isEmpty();
    }
    return eval;
  }

  @Override
  public Issue getRejectedReason() {
    return (missingFields.isEmpty()) ? null : new Issue(MISSING_REQUIRED_FIELDS_KEY, MISSING_REQUIRED_FIELDS_DEFAULT,
                                                        missingFields);
  }

}
