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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.ext.JsonObjectReader;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.ext.io.OverrunException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;

public class JsonObjectReaderImpl implements JsonObjectReader {
  private static final Logger LOG = LoggerFactory.getLogger(JsonObjectReaderImpl.class);

  private static final int MAX_CHARS_TO_READ_FORWARD = 64;
  private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();

  private final Reader reader;
  private final JsonParser jsonParser;
  private final Mode mode;
  private final Class objectClass;
  private final ObjectMapper objectMapper;

  private boolean starting;
  private JsonStreamContext rootContext;
  private long posCorrection;
  private boolean closed;
  private Byte firstNonSpaceChar;
  private JsonToken nextToken;

  public JsonObjectReaderImpl(Reader reader, Mode mode) throws IOException {
    this(reader, 0, mode);
  }

  public JsonObjectReaderImpl(Reader reader, long initialPosition, Mode mode) throws IOException {
    this(reader, initialPosition, mode, Object.class, DEFAULT_OBJECT_MAPPER);
  }

  public JsonObjectReaderImpl(Reader reader, long initialPosition, Mode mode, Class<?> objectClass) throws IOException {
    this(reader, initialPosition, mode, objectClass, DEFAULT_OBJECT_MAPPER);
  }

  public JsonObjectReaderImpl(Reader reader, long initialPosition, Mode mode, Class<?> objectClass, ObjectMapper objectMapper) throws IOException {
    this.mode = mode;
    this.objectClass = objectClass;
    this.objectMapper = objectMapper;

    starting = true;
    this.reader = reader;
    if (mode == Mode.MULTIPLE_OBJECTS) {
      if (initialPosition > 0) {
        IOUtils.skipFully(reader, initialPosition);
        posCorrection += initialPosition;
      }
      if (reader.markSupported()) {
        reader.mark(MAX_CHARS_TO_READ_FORWARD);
        int count = 0;
        byte firstByte = -1;
        while (count++ < MAX_CHARS_TO_READ_FORWARD &&
            (firstByte = (byte)reader.read()) != -1 &&
            firstByte <= ' '); // everything less than a space is whitespace
        if (firstByte > ' ') {
          firstNonSpaceChar = firstByte;
        }
        reader.reset();
      }
    }
    jsonParser = getObjectMapper().getFactory().createParser(reader);
    if (mode == Mode.ARRAY_OBJECTS && initialPosition > 0) {
      fastForwardJsonParser(initialPosition);
    }
  }

  @Override
  public Object read() throws IOException {
    if (closed) {
      throw new IOException("The parser is closed");
    }
    try {
      Object value = null;
      switch (mode) {
        case ARRAY_OBJECTS:
          value = readObjectFromArray();
          break;
        case MULTIPLE_OBJECTS:
          value = readObjectFromStream();
          break;
      }
      return value;
    } catch (RuntimeJsonMappingException ex) {
      throw new JsonParseException(ex.toString(), jsonParser.getTokenLocation(), ex);
    }
  }

  @Override
  public long getReaderPosition() {
    // Jackson 2.7+ will now return -1 for token location if nextToken has not yet been called rather than 0
    // This change broke our usage and so taking the max will return the value we expect.
    return (mode == Mode.ARRAY_OBJECTS) ? Math.max(jsonParser.getTokenLocation().getCharOffset(), 0)
        : Math.max(jsonParser.getTokenLocation().getCharOffset(), 0) + posCorrection;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    jsonParser.close();
  }

  ObjectMapper getObjectMapper() {
    return objectMapper;
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
    // no-op
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
  protected Object readObjectFromArray() throws IOException {
    Object value = JsonObjectReader.EOF;
    if (starting) {
      starting = false;
      JsonToken token = jsonParser.nextToken();
      rootContext = jsonParser.getParsingContext();
      if (token != JsonToken.START_ARRAY) {
        throw new JsonParseException(
            Utils.format("JSON array expected but stream starts with '{}'", token),
            jsonParser.getTokenLocation());
      }
    }
    JsonToken token = jsonParser.nextToken();
    if (token != null && token != JsonToken.END_ARRAY) {
      value = jsonParser.readValueAs(Object.class);
    }
    return value;
  }

  @Override
  public Class getExpectedClass() {
    return objectClass;
  }

  @SuppressWarnings("unchecked")
  protected Object readObjectFromStream() throws IOException {
    Object value = JsonObjectReader.EOF;
    if (starting) {
      starting = false;
      nextToken = jsonParser.nextToken();
    }
    if (nextToken != null) {
      value = jsonParser.readValueAs(getExpectedClass());
      nextToken = jsonParser.nextToken();
      if (nextToken == null) {
        // if we reached the EOF Jackson JSON parser keeps the as getTokenLocation() the location of the last token,
        // we need to adjust by 1 to make sure we are after it if getReaderPosition() is called
        posCorrection++;
      }
    }
    return value;
  }

  protected void fastForwardToNextRootObject() throws IOException {
    Preconditions.checkState(mode == Mode.MULTIPLE_OBJECTS, "Parser must be in MULTIPLE_OBJECT mode");
    JsonToken token = jsonParser.getCurrentToken();
    try {
      if (token == null) {
        token = jsonParser.nextToken();
      }
      while (token != null && !jsonParser.getParsingContext().inRoot()) {
        token = jsonParser.nextToken();
      }
      nextToken = jsonParser.nextToken();
    } catch (OverrunException e) {
      if (firstNonSpaceChar == null || firstNonSpaceChar != '[') {
        throw e;
      } else {
        String msg = Utils.format("Overrun exception occurred on a file starting with '[', which may " +
            "indicate the incorrect processor mode '{}' is being used: {}", mode.name(), e);
        throw new OverrunException(msg, e.getStreamOffset(), e);
      }
    }
  }

}
