/*
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

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;

public class JsonRecordWriterImpl implements JsonRecordWriter {
  private final static String EOL = System.getProperty("line.separator");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JsonFactory JSON_FACTORY = OBJECT_MAPPER.getFactory();

  static {
    OBJECT_MAPPER.disable(SerializationFeature.FLUSH_AFTER_WRITE_VALUE);
    JSON_FACTORY.setRootValueSeparator(EOL);
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
    writeFieldToJsonObject(record.get());
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

  private void writeFieldToJsonObject(Field field) throws IOException {
    if (field == null || field.getValue() == null) {
      generator.writeNull();
      return;
    }
    switch (field.getType()) {
      case FILE_REF:
        throw new IOException("Cannot serialize FileRef fields.");
      case MAP:
      case LIST_MAP:
        generator.writeStartObject();
        Map<String, Field> map = field.getValueAsMap();
        for (Map.Entry<String, Field> fieldEntry : map.entrySet()) {
          generator.writeFieldName(fieldEntry.getKey());
          writeFieldToJsonObject(fieldEntry.getValue());
        }
        generator.writeEndObject();
        break;
      case LIST:
        generator.writeStartArray();
        List<Field> list = field.getValueAsList();
        for (Field f : list) {
          writeFieldToJsonObject(f);
        }
        generator.writeEndArray();
        break;
      case BOOLEAN:
        generator.writeBoolean(field.getValueAsBoolean());
        break;
      case CHAR:
        generator.writeString(String.valueOf(field.getValueAsChar()));
        break;
      case BYTE:
        generator.writeBinary(new byte[] {field.getValueAsByte()});
        break;
      case SHORT:
        generator.writeNumber(field.getValueAsShort());
        break;
      case INTEGER:
        generator.writeNumber(field.getValueAsInteger());
        break;
      case LONG:
        generator.writeNumber(field.getValueAsLong());
        break;
      case FLOAT:
        generator.writeNumber(field.getValueAsFloat());
        break;
      case DOUBLE:
        generator.writeNumber(field.getValueAsDouble());
        break;
      case DATE:
        generator.writeNumber(field.getValueAsDate().getTime());
        break;
      case DATETIME:
        generator.writeNumber(field.getValueAsDatetime().getTime());
        break;
      case TIME:
        generator.writeNumber(field.getValueAsTime().getTime());
        break;
      case DECIMAL:
        generator.writeNumber(field.getValueAsDecimal());
        break;
      case STRING:
        generator.writeString(field.getValueAsString());
        break;
      case BYTE_ARRAY:
        generator.writeBinary(field.getValueAsByteArray());
        break;
      case ZONED_DATETIME:
        generator.writeString(field.getValueAsString());
        break;
      default:
        throw new IllegalStateException(String.format(
            "Unrecognized field type (%s) in field: %s",
            field.getType().name(),
            field.toString())
        );
    }
  }
}
