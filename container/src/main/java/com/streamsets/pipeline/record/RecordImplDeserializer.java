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
package com.streamsets.pipeline.record;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.streamsets.pipeline.api.Field;

import java.io.IOException;
import java.util.Map;

// we need this for stage preview runs, otherwise Jackson tries to be smart and things blow because of
// properties using interfaces
public class RecordImplDeserializer extends JsonDeserializer<RecordImpl> {

  @Override
  @SuppressWarnings("unchecked")
  public RecordImpl deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
    Map map = jp.readValueAs(Map.class);
    SimpleMap<String, Object> header = convertToHeaderSimpleMap((Map<String, Object>) map.get("header"));
    SimpleMap<String, Field> fields = convertToFieldSimpleMap((Map<String, Object>) map.get("values"));
    return new RecordImpl(header, fields);
  }

  @SuppressWarnings("unchecked")
  private static SimpleMap<String, Object> convertToHeaderSimpleMap(Map<String, ?> map) {
    SimpleMap<String, Object> headers = new VersionedSimpleMap<String, Object>();
    headers.put(RecordImpl.STAGE_CREATOR_INSTANCE_ATTR, map.get("stageCreator"));
    headers.put(RecordImpl.RECORD_SOURCE_ID_ATTR, map.get("sourceId"));
    headers.put(RecordImpl.TRACKING_ID_ATTR, map.get("trackingId"));
    if(map.get("previousStageTrackingId") != null) {
      headers.put(RecordImpl.PREVIOUS_STAGE_TRACKING_ID_ATTR, map.get("previousStageTrackingId"));
    }
    headers.put(RecordImpl.STAGES_PATH_ATTR, map.get("stagesPath"));
    if (map.get("raw") != null) {
      headers.put(RecordImpl.RAW_DATA_ATTR, map.get("raw"));
      headers.put(RecordImpl.RAW_MIME_TYPE_ATTR, map.get("rawMimeType"));
    }
    for (Map.Entry entry : ((Map<String, ?>)map.get("values")).entrySet()) {
      headers.put((String) entry.getKey(), entry.getValue());
    }
    return headers;
  }

  private static Field parse(Map map) {
    Field.Type type = Field.Type.valueOf((String) map.get("type"));
    Object value = map.get("value");
    return Field.create(type, value);
  }

  @SuppressWarnings("unchecked")
  private static SimpleMap<String, Field> convertToFieldSimpleMap(Map<String, ?> map) {
    SimpleMap<String, Field> fields = new VersionedSimpleMap<String, Field>();
    for (Map.Entry entry : map.entrySet()) {
      fields.put((String) entry.getKey(), parse((Map) entry.getValue()));
    }
    return fields;
  }

}
