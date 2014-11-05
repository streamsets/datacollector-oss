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

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

import java.util.Random;

@StageDef(name = "randomSource", version="1.0.0", label="Random Record Source")
public class RandomSource extends BaseSource {

  @ConfigDef(name="recordFields", required = true, type = ConfigDef.Type.STRING,
             label = "Record fields to generate, comma separated")
  public String fields;

  private String[] fieldArr;
  private Random random;

  @Override
  protected void init() throws StageException {
    fieldArr = fields.split(",");
    random = new Random();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    maxBatchSize = (maxBatchSize > -1) ? maxBatchSize : 10;
    for (int i = 0; i < maxBatchSize; i++ ) {
      batchMaker.addRecord(createRecord(lastSourceOffset, i));
    }
    return "random";
  }

  private Record createRecord(String lastSourceOffset, int batchOffset) {
    Record record = getContext().createRecord("random:" + batchOffset);
    for (String field : fieldArr) {
      record.setField(field, Field.create(random.nextLong()));
    }
    return record;
  }
}
