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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JsonCharDataGenerator implements DataGenerator {
  final static String EOL = System.getProperty("line.separator");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JsonFactory JSON_FACTORY = OBJECT_MAPPER.getFactory();

  private final boolean isArray;
  private final JsonGenerator generator;
  private boolean closed;

  public JsonCharDataGenerator(Writer writer, JsonMode jsonMode) throws IOException {
    isArray = jsonMode == JsonMode.ARRAY_OBJECTS;
    generator = JSON_FACTORY.createGenerator(writer);
    if (isArray) {
      generator.writeStartArray();
    }
  }

  //VisibleForTesting
  boolean isArrayObjects() {
    return isArray;
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    if (closed) {
      throw new IOException("generator has been closed");
    }
    generator.writeObject(fieldToJsonObject(record, record.get()));
    if (!isArray) {
      generator.writeRaw(EOL);
    }
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("generator has been closed");
    }
    generator.flush();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    if (isArray) {
      generator.writeEndArray();
    }
    generator.close();
  }

  public static Object fieldToJsonObject(Record record, Field field) throws DataGeneratorException {
    Object obj;
    if (field == null || field.getValue() == null) {
      obj = null;
    } else if (field.getType() == Field.Type.LIST) {
      List<Field> list = field.getValueAsList();
      List<Object> toReturn = new ArrayList<>(list.size());
      for (Field f : list) {
        toReturn.add(fieldToJsonObject(record, f));
      }
      obj = toReturn;
    } else if (field.getType() == Field.Type.MAP || field.getType() == Field.Type.LIST_MAP) {
      Map<String, Field> map = field.getValueAsMap();
      Map<String, Object> toReturn = new LinkedHashMap<>();
      for (Map.Entry<String, Field> entry : map.entrySet()) {
        toReturn.put(entry.getKey(), fieldToJsonObject(record, entry.getValue()));
      }
      obj = toReturn;
    } else {
      obj = field.getValue();
    }
    return obj;
  }
}
