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
package com.streamsets.datacollector.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.File;
import java.io.IOException;

public class JSON {
  private ObjectMapper mapper;

  public JSON() {
    mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING);
    mapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);
	}

  /**
   * Serialize the given Java object into JSON string.
   */
  public String serialize(Object obj) throws ApiException {
    try {
      if (obj != null)
        return mapper.writeValueAsString(obj);
      else
        return null;
    } catch (Exception e) {
      throw new ApiException(400, e.getMessage());
    }
  }

  /**
   * Deserialize the given JSON string to Java object.
   *
   * @param body The JSON string
   * @param returnType The type to deserialize inot
   * @return The deserialized Java object
   */
  public <T> T deserialize(String body, TypeRef returnType) throws ApiException {
    JavaType javaType = mapper.constructType(returnType.getType());
    try {
      return mapper.readValue(body, javaType);
    } catch (IOException e) {
      if (returnType.getType().equals(String.class))
        return (T) body;
      else
        throw new ApiException(500, e.getMessage(), null, body);
    }
  }

  /**
   * Deserialize the given File to Java object.
   *
   * @param file The File path
   * @param returnType The type to deserialize inot
   * @return The deserialized Java object
   */
  public <T> T deserialize(File file, TypeRef returnType) throws ApiException {
    JavaType javaType = mapper.constructType(returnType.getType());
    try {
      return mapper.readValue(file, javaType);
    } catch (IOException e) {
      throw new ApiException(500, e.getMessage(), null, "File to read file");
    }
  }

  public ObjectMapper getMapper() {
    return mapper;
  }

}
