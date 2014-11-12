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
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.validation.Issue;

public class RequiredFieldsErrorSink implements FilterRecordBatch.Sink {
  private final String instanceName;
  private final ErrorRecordSink errorSink;
  private int counter;

  public RequiredFieldsErrorSink(String instanceName, ErrorRecordSink errorSink) {
    this.instanceName = instanceName;
    this.errorSink = errorSink;
  }

  @Override
  public void add(Record record, Issue issue) {
    errorSink.addRecord(instanceName, new ErrorRecord(record, issue));
    counter++;
  }

  public int size() {
    return counter;
  }

  @Override
  public String toString() {
    return Utils.format("RequiredFieldsErrorSink[size='{}']", size());
  }

}
