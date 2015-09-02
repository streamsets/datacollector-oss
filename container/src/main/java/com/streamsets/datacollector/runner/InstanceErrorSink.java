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
package com.streamsets.datacollector.runner;

import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;

public class InstanceErrorSink implements FilterRecordBatch.Sink {;
  private final String instanceName;
  private final ErrorSink errorSink;
  private int counter;

  public InstanceErrorSink(String instanceName, ErrorSink errorSink) {
    this.instanceName = instanceName;
    this.errorSink = errorSink;
  }

  @Override
  public void add(Record record, ErrorMessage reason) {
    RecordImpl recordImpl = (RecordImpl) record;
    recordImpl.getHeader().setError(instanceName, reason);
    errorSink.addRecord(instanceName, recordImpl);
    counter++;
  }

  public int size() {
    return counter;
  }


  @Override
  public String toString() {
    return Utils.format("InstanceErrorSink[instance='{}' counter='{}']", instanceName, counter);
  }

}
