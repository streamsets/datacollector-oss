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
package com.streamsets.pipeline.api.v3.module;

import com.streamsets.pipeline.api.v3.Batch;
import com.streamsets.pipeline.api.v3.BatchMaker;
import com.streamsets.pipeline.api.v3.Processor;
import com.streamsets.pipeline.api.v3.Processor.Context;
import com.streamsets.pipeline.api.v3.record.Record;

import java.util.Iterator;

public abstract class SelectorModule extends BaseModule<Context> implements Processor {

  @Override
  public final void process(Batch batch, BatchMaker batchMaker) {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      String tag = select(record);
      if (tag == null) {
        batchMaker.addRecord(record);
      } else {
        batchMaker.addRecord(record, tag);
      }
    }
  }

  protected abstract String select(Record record);

}
