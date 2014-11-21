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
package com.streamsets.pipeline.lib.json;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.streamsets.pipeline.container.Utils;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Parses objects from a stream in streaming mode. Limited to primitive types and collections.
 */
public class StreamingJsonParser {

  public static enum Mode {ARRAY_OBJECTS, MULTIPLE_OBJECTS}

  private final Reader reader;
  private final JsonParser jsonParser;
  private final Mode mode;
  private boolean starting = true;
  private JsonStreamContext rootContext;
  private Iterator<?> multipleObjectsIterator;

  protected ObjectMapper getObjectMapper() {
    return new ObjectMapper();
  }

  public StreamingJsonParser(Reader reader, Mode mode) throws IOException {
    this.reader = reader;
    jsonParser = getObjectMapper().getFactory().createParser(reader);
    this.mode = mode;
  }

  protected Reader getReader() {
    return reader;
  }

  protected JsonParser getJsonParser() {
    return jsonParser;
  }

  // not null only if in ARRAY_OBJECT mode
  protected JsonStreamContext getRootContext() {
    return rootContext;
  }

  protected <T> T readObjectFromArray(Class<T> klass) throws IOException {
    T value = null;
    if (starting) {
      starting = false;
      JsonToken token = jsonParser.nextToken();
      rootContext = jsonParser.getParsingContext();
      if (token != JsonToken.START_ARRAY) {
        throw new JsonParseException(Utils.format("JSON array expected but stream starts with '{}'", token),
                                     jsonParser.getTokenLocation());
      }
    }
    JsonToken token = jsonParser.nextToken();
    if (token != null) {
      if (token != JsonToken.END_ARRAY) {
        value = jsonParser.readValueAs(klass);
      }
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  protected <T> T readObjectFromStream(Class<T> klass) throws IOException {
    if (multipleObjectsIterator == null) {
      multipleObjectsIterator = jsonParser.readValuesAs(klass);
    }
    return (T) ((multipleObjectsIterator.hasNext()) ? multipleObjectsIterator.next() : null);
  }

  protected void resetMultipleObjectIterator() {
    multipleObjectsIterator = null;
  }

  private <T> T read(Class<T> klass) throws IOException {
    try {
      T value = null;
      switch (mode) {
        case ARRAY_OBJECTS:
          value = readObjectFromArray(klass);
          break;
        case MULTIPLE_OBJECTS:
          value = readObjectFromStream(klass);
          break;
      }
      return value;
    } catch (RuntimeJsonMappingException ex) {
      throw new JsonParseException(ex.getMessage(), jsonParser.getTokenLocation(), ex);
    }
  }

  public Map readMap() throws IOException {
    return read(Map.class);
  }

  public List readList() throws IOException {
    return read(List.class);
  }

}
