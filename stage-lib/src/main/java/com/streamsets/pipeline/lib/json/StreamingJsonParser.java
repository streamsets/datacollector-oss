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
import org.apache.commons.io.IOUtils;

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
  private boolean starting;
  private JsonStreamContext rootContext;
  private Iterator<?> multipleObjectsIterator;
  private long posCorrection;

  protected ObjectMapper getObjectMapper() {
    return new ObjectMapper();
  }

  public StreamingJsonParser(Reader reader, Mode mode) throws IOException {
    this(reader, 0, mode);
  }

  public StreamingJsonParser(Reader reader, long initialPosition, Mode mode) throws IOException {
    starting = true;
    this.reader = reader;
    if (mode == Mode.MULTIPLE_OBJECTS && initialPosition > 0) {
      IOUtils.skipFully(reader, initialPosition);
      posCorrection += initialPosition;
    }
    jsonParser = getObjectMapper().getFactory().createParser(reader);
    if (mode == Mode.ARRAY_OBJECTS && initialPosition > 0) {
      fastForwardJsonParser(initialPosition);
    }
    this.mode = mode;
  }

  private void fastForwardJsonParser(long initialPosition) throws IOException {
    while (jsonParser.getTokenLocation().getCharOffset() < initialPosition) {
      jsonParser.nextToken();
      if (starting) {
        rootContext = jsonParser.getParsingContext();
        starting = false;
      }
      fastForwardLeaseReader();
    }
  }

  protected void fastForwardLeaseReader() {
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

  @SuppressWarnings("unchecked")
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
        Class classToUse = klass;
        if (klass == Object.class) {
          switch (token) {
            case START_ARRAY:
              classToUse = List.class;
              break;
            case START_OBJECT:
              classToUse = Map.class;
              break;
            default:
              throw new JsonParseException(Utils.format("JSON array elements must be ARRAY or MAP, found '{}'",
                                                        token), jsonParser.getTokenLocation());
          }
        }
        value = (T) jsonParser.readValueAs(classToUse);
      }
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  protected <T> T readObjectFromStream(Class<T> klass) throws IOException {
    if (multipleObjectsIterator == null) {
      Class classToUse = klass;
      if (klass == Object.class) {
        JsonToken token = (jsonParser.hasCurrentToken()) ? jsonParser.getCurrentToken() : jsonParser.nextToken();
        switch (token) {
          case START_ARRAY:
            classToUse = List.class;
            break;
          case START_OBJECT:
            classToUse = Map.class;
            break;
          default:
            throw new JsonParseException(Utils.format("JSON elements must be ARRAY or MAP, found '{}'",
                                                      token), jsonParser.getTokenLocation());
        }
      }
      multipleObjectsIterator = jsonParser.readValuesAs(classToUse);
    }
    return (T) ((multipleObjectsIterator.hasNext()) ? multipleObjectsIterator.next() : null);
  }

  protected void resetMultipleObjectIterator() {
    multipleObjectsIterator = null;
  }

  public long getReaderPosition() {
    return (mode == Mode.ARRAY_OBJECTS) ? jsonParser.getTokenLocation().getCharOffset()
                                        : jsonParser.getCurrentLocation().getCharOffset() + posCorrection;
  }

  public <T> T read(Class<T> klass) throws IOException {
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

  public Object read() throws IOException {
    return read(Object.class);
  }

  public Map readMap() throws IOException {
    return read(Map.class);
  }

  public List readList() throws IOException {
    return read(List.class);
  }

}
