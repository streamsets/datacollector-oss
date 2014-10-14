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

import com.streamsets.pipeline.api.v1.AbstractRecordProcessor;
import com.streamsets.pipeline.api.v1.Configuration;
import com.streamsets.pipeline.api.v1.Metadata;
import com.streamsets.pipeline.api.v1.Metadata.Builder;
import com.streamsets.pipeline.api.v1.Metadata.Type;
import com.streamsets.pipeline.api.v1.ModuleDef;
import com.streamsets.pipeline.api.v1.Record;
import com.streamsets.pipeline.api.v1.RecordEditor;

@ModuleDef(name = "TypeConvertorProcessor", version = "0.1.0",
           description = "Converts String fields to other types")
public class StringToIntegerRecordProcessor extends AbstractRecordProcessor {

  @Configuration(name = "fields-to-handle", type = Configuration.Type.STRING, defaultValue = {"age", "weight"})
  private String[] fieldsToConvert;


  private Metadata[] fieldsMetadata;

  @Override
  protected void init() {
    fieldsMetadata = new Metadata[fieldsToConvert.length];
    Metadata.Builder builder = new Metadata.Builder();
    builder.setFormat("%d");
    builder.setType(Type.INTEGER);
    for (int i = 0; i < fieldsMetadata.length; i++) {
      String field = fieldsToConvert[i];
      builder.setName(field);
      fieldsMetadata[i] = builder.build();
    }
  }

  @Override
  protected Record process(Record record) {
    RecordEditor recordE = new RecordEditor(record);
    for (Metadata fieldMetada : fieldsMetadata) {
      if (record.hasField(fieldMetada.getName())) {
        recordE.setValue(fieldMetada.getName(), fieldMetada,
                         Integer.parseInt(record.getValue(fieldMetada.getName(), String.class)));
      }
    }
    return record;
  }


}


