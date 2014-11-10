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
package com.streamsets.pipeline.lib.basics;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;

import java.util.Iterator;

@StageDef(name = "recordCreator", version = "1.0.0", label = "Identity",
          description = "It creates 2 records from each original record")
public class RecordCreatorProcessor extends SingleLaneProcessor {

  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws
      StageException {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      Record record1 = getContext().createRecord(record);
      Record record2 = getContext().createRecord(record);
      Iterator<String> fIt = record.getFieldNames();
      while (fIt.hasNext()) {
        String name = fIt.next();
        record1.setField(name, record.getField(name));
        record2.setField(name, record.getField(name));
      }
      record1.setField("expanded", Field.create(1));
      record2.setField("expanded", Field.create(2));
      batchMaker.addRecord(record1);
      batchMaker.addRecord(record2);
    }
  }

}
