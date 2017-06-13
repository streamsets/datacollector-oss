/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.JsonRecordWriter;
import com.streamsets.pipeline.api.ext.json.Mode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JsonRecordWriterImpl implements JsonRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(JsonRecordWriter.class);

  private final static String EOL = System.getProperty("line.separator");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JsonFactory JSON_FACTORY = OBJECT_MAPPER.getFactory();

  static {
    OBJECT_MAPPER.disable(SerializationFeature.FLUSH_AFTER_WRITE_VALUE);
  }

  private final JsonGenerator generator;
  private final boolean isArray;

  private boolean closed;

  public JsonRecordWriterImpl(Writer writer, Mode mode) throws IOException {
    isArray = mode == Mode.ARRAY_OBJECTS;
    generator = JSON_FACTORY.createGenerator(writer);
    if (isArray) {
      generator.writeStartArray();
    }
  }

  @Override
  public void write(Record record) throws IOException {
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

  private static Object fieldToJsonObject(Record record, Field field) throws IOException {
    Object obj;
    if (field == null || field.getValue() == null) {
      obj = null;
    } else if(field.getType() == Field.Type.FILE_REF) {
      throw new IOException("Cannot serialize FileRef fields.");
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
