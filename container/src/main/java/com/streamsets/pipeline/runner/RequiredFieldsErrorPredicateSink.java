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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.util.Message;
import com.streamsets.pipeline.util.PipelineException;

import java.util.ArrayList;
import java.util.List;

//FIXME
public class RequiredFieldsErrorPredicateSink implements FilterRecordBatch.Predicate, FilterRecordBatch.Sink {

  private static final String MISSING_REQUIRED_FIELDS_KEY = "missing.required.fields";

  private final List<String> requiredFields;
  private final String instanceName;
  private final ErrorRecordSink errorSink;
  private final List<String> missingFields;
  private int counter;

  public RequiredFieldsErrorPredicateSink(String instanceName, List<String> requiredFields, ErrorRecordSink errorSink) {
    this.requiredFields = requiredFields;
    this.instanceName = instanceName;
    this.errorSink = errorSink;
    missingFields = new ArrayList<String>();
  }

  @Override
  public void add(Record record, Message msg) {
//    errorSink.addRecord(instanceName, new ErrorRecord(record, ERROR.MISSING_FIELDS, msg));
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
  public StageException.ID getRejectedReason() {
    return null; //MISSING_FIELDS;
  }

  @Override
  public Message getRejectedMessage() {
    return null;
//    return (missingFields.isEmpty()) ? null : new Message(
//        MISSING_REQUIRED_FIELDS_KEY .MISSING_FIELDS.getMessageTemplate(), missingFields);
  }

  @Override
  public String toString() {
    return Utils.format("RequiredFieldsPredicateSink[fields='{}' size='{}']", requiredFields, size());
  }

}
