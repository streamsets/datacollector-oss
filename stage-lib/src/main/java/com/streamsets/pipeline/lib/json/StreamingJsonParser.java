/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.json;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.container.Utils;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.Reader;

/**
 * Parses JSON objects from a stream in streaming mode.
 */
public class StreamingJsonParser {

  public static enum Mode {ARRAY_OBJECTS, MULTIPLE_OBJECTS}

  private final Reader reader;
  private final JsonParser jsonParser;
  private final Mode mode;
  private boolean starting;
  private JsonStreamContext rootContext;
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
  protected Object readObjectFromArray() throws IOException {
    Object value = null;
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
        value = jsonParser.readValueAs(Object.class);
      }
    }
    return value;
  }

  private JsonToken nextToken;

  @SuppressWarnings("unchecked")
  protected Object readObjectFromStream() throws IOException {
    Object value = null;
    if (starting) {
      starting = false;
      nextToken = jsonParser.nextToken();
    }
    if (nextToken != null) {
      value = jsonParser.readValueAs(Object.class);
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
    if (token == null) {
      token = jsonParser.nextToken();
    }
    while (token != null && !jsonParser.getParsingContext().inRoot()) {
      token = jsonParser.nextToken();
    }
    nextToken = jsonParser.nextToken();
  }

  public long getReaderPosition() {
    return (mode == Mode.ARRAY_OBJECTS) ? jsonParser.getTokenLocation().getCharOffset()
                                        : jsonParser.getTokenLocation().getCharOffset() + posCorrection;
  }

  public Object read() throws IOException {
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
      throw new JsonParseException(ex.getMessage(), jsonParser.getTokenLocation(), ex);
    }
  }

}
