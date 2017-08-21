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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.ext.json.JsonMapper;

import java.io.IOException;

public class JsonMapperImpl implements JsonMapper {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public String writeValueAsString(Object value) throws IOException {
    return OBJECT_MAPPER.writeValueAsString(value);
  }

  @Override
  public byte[] writeValueAsBytes(Object value) throws IOException {
    return OBJECT_MAPPER.writeValueAsBytes(value);
  }

  @Override
  public <T> T readValue(byte[] src, Class<T> valueType) throws IOException {
    return OBJECT_MAPPER.readValue(src, valueType);
  }

  @Override
  public <T> T readValue(String src, Class<T> valueType) throws IOException {
    return OBJECT_MAPPER.readValue(src, valueType);
  }

  public boolean isValidJson(String json) {
    try {
      OBJECT_MAPPER.readTree(json);
    } catch (IOException ignored) { // NOSONAR
      return false;
    }
    return true;
  }
}
