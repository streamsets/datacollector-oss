/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.lib.util.JsonUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.postgresql.replication.LogSequenceNumber;

public class PostgresWalRecord {

  private final DecoderValues decoder;
  private ByteBuffer buffer;
  private Field field;
  private LogSequenceNumber lsn;

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public LogSequenceNumber getLsn() {
    return lsn;
  }

  public void setLsn(LogSequenceNumber lsn) {
    this.lsn = lsn;
  }

  public DecoderValues getDecoder() {
    return decoder;
  }

  public PostgresWalRecord(ByteBuffer buffer, LogSequenceNumber lsn, DecoderValues decoder) {
    this.buffer = buffer;
    this.lsn = lsn;
    this.field = null; //converter throws exception best handled in getter
    this.decoder = decoder;
  }

  public PostgresWalRecord(PostgresWalRecord record, Field changes) {
    this.buffer = record.getBuffer();
    this.lsn = record.getLsn();
    this.field = record.getField();
    this.setChanges(changes);
    this.decoder = record.getDecoder();
  }

  private String bufferToString() {
    int offset = buffer.arrayOffset();
    byte[] source = buffer.array();
    int length = source.length - offset;
    return new String(source, offset, length);
  }

  public String toString() {
    return bufferToString() + " LSN: " + lsn.asString();
  }

  public Field getField() {
    if (field == null) {
      if (decoder == null) {
        return field;
      }
      switch(decoder) {

        case WAL2JSON:
          JsonNode json;
          try {
            ObjectMapper om = new ObjectMapper();
            ObjectReader reader = om.reader();
            json = reader.readTree(bufferToString());

            field = JsonUtil.jsonToField(om.convertValue(json, Map.class));
          } catch (IOException e) {
            field = null;
          }
          break;

        default:
          field = null; //should be null but here explicitly to highlight
          break;
      }

    }

    return field;
  }

  private Field getEntryFromField(String key) {
    Map<String, Field> valueAsMap = getField().getValueAsMap();
    if (valueAsMap.get(key) != null) {
      return valueAsMap.get(key);
    } else {
      return null;
    }
  }

  public String getXid() {
    Field entry = getEntryFromField("xid");
    return entry.getValueAsString();
  }

  public String getNextLSN() {
    Field entry  = getEntryFromField("nextlsn");
    return entry.getValueAsString();
  }

  public String getTimestamp() {
    Field entry = getEntryFromField("timestamp");
    return entry.getValueAsString();
  }

  public List<Field> getChanges() {
    //Will return a 0...n length Field.LIST
    Field entry  = getEntryFromField("change");
    return entry.getValueAsList();
  }

  private void setChanges(Field entry) {
    Map<String, Field> valueAsMap = field.getValueAsMap();
    valueAsMap.replace("change", entry);
  }

  public static String getTypeFromChangeMap(Map<String, Field> change) {
    return change.get("kind").getValueAsString();
  }

  public static String getSchemaFromChangeMap(Map<String, Field> change) {
    return change.get("schema").getValueAsString();
  }

  public static String getTableFromChangeMap(Map<String, Field> change) {
    return change.get("table").getValueAsString();
  }

}
