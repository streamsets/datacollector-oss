/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.generator.json;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;

import java.io.IOException;
import java.io.Writer;

public class JsonCharDataGenerator implements DataGenerator {

  private static final String SDC_JSON_GENERATOR_BOON_ENABLED_KEY = "sdc.json.generator.boon.enabled";
  private static final String SDC_JSON_GENERATOR_BOON_ENABLED_DEFAULT = "false";

  private DataGenerator dataGenerator;
  private boolean isArray;

  public JsonCharDataGenerator(Writer writer, JsonMode jsonMode)
      throws IOException {
    if(jsonMode == JsonMode.ARRAY_OBJECTS) {
      isArray = true;
      dataGenerator = new JacksonCharDataGenerator(writer, jsonMode);
    } else {
      String property = System.getProperty(SDC_JSON_GENERATOR_BOON_ENABLED_KEY, SDC_JSON_GENERATOR_BOON_ENABLED_DEFAULT);
      if("true".equalsIgnoreCase(property)) {
        dataGenerator = new BoonCharDataGenerator(writer, jsonMode);
      } else {
        dataGenerator = new JacksonCharDataGenerator(writer, jsonMode);
      }
    }
  }

  //VisibleForTesting
  boolean isArrayObjects() {
    return isArray;
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    dataGenerator.write(record);
  }

  @Override
  public void flush() throws IOException {
    dataGenerator.flush();
  }

  @Override
  public void close() throws IOException {
    dataGenerator.close();
  }

}
