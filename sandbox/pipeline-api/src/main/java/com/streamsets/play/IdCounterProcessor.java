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
package com.streamsets.play;

import com.codahale.metrics.Meter;
import com.streamsets.pipeline.api.v1.AbstractProcessor;
import com.streamsets.pipeline.api.v1.Batch;
import com.streamsets.pipeline.api.v1.BatchMaker;
import com.streamsets.pipeline.api.v1.Configuration;
import com.streamsets.pipeline.api.v1.ModuleDef;
import com.streamsets.pipeline.api.v1.Record;

import java.util.HashSet;
import java.util.Set;

@ModuleDef(name = "IdCounter", version = "0.1.0",
           description = "Counts how many different IDs go through per batch")
public class IdCounterProcessor extends AbstractProcessor {

  @Configuration(name = "idField", type = Configuration.Type.STRING, defaultValue = "id")
  private String idField;

  private Meter meter;

  @Override
  protected void init() {
    meter = new Meter();
    getContext().getMetric().register(getInfo().getInstance() + ":idCounter.meter", meter);
  }

  @Override
  public void process(Batch batch, BatchMaker batchMaker) {
    Set<String> ids = new HashSet<String>();
    for (Record record : batch.getAllRecords()) {
      String id = record.getValue(idField, String.class);
      ids.add(id);
      batchMaker.addRecord(record);
    }
    meter.mark(ids.size());
  }

}


