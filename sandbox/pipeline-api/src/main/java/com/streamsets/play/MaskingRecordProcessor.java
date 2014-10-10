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
import com.streamsets.pipeline.api.v1.ModuleDef;
import com.streamsets.pipeline.api.v1.Record;
import com.streamsets.pipeline.api.v1.RecordEditor;

@ModuleDef(name = "MaskingProcessor", version = "0.1.0",
           description = "Masks specified record fields")
public class MaskingRecordProcessor extends AbstractRecordProcessor {
  private static final String MASK = "******";

  @Configuration(name = "fields-to-mask", type = Configuration.Type.STRING, defaultValue = {"ssn", "dob"})
  private String[] fieldsToMask;

  @Override
  protected Record process(Record record) {
    RecordEditor recordE = new RecordEditor(record);
    for (String field : fieldsToMask) {
      if (record.hasField(field)) {
        recordE.setValue(field, MASK);
      }
    }
    return record;
  }


}


