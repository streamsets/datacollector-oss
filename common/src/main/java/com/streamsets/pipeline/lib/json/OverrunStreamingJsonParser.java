/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.io.ObjectLengthException;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.util.ExceptionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Caps amount the size of of JSON objects being parsed, discarding the ones that exceed the limit and fast forwarding
 * to the next.
 * <p/>
 * If the max object length is exceed, the readMap(), readList() method will throw a JsonObjectLengthException.
 * After a JsonObjectLengthException exception the parser is still usable, the subsequent read will be positioned for
 * the next object.
 * <p/>
 * The underlying InputStream is wrapped with an OverrunInputStream to prevent an overrun due to an extremely large
 * field name or string value. The default limit is 100K and it is configurable via a JVM property,
 * {@link com.streamsets.pipeline.lib.io.OverrunReader#READ_LIMIT_SYS_PROP}, as it is not expected user will need
 * to change this. If an OverrunException is thrown the parser is not usable anymore.
 */
public class OverrunStreamingJsonParser extends StreamingJsonParser {

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    SimpleModule module = new SimpleModule();
    module.addDeserializer(Map.class, new MapDeserializer());
    module.addDeserializer(List.class, new ListDeserializer());
    OBJECT_MAPPER.registerModule(module);
    OBJECT_MAPPER.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    OBJECT_MAPPER.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
  }

  public static class EnforcerMap extends LinkedHashMap {

    @Override
    @SuppressWarnings("unchecked")
    public Object put(Object key, Object value) {
      try {
        return super.put(key, value);
      } finally {
        checkIfLengthExceededForObjectRead(this);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public String toString() {
      StringBuilder sb = new StringBuilder("{");
      String separator = "";
      for (Map.Entry entry : (Set<Map.Entry>) entrySet()) {
        sb.append(separator).append(entry.getKey()).append("=").append(entry.getValue());
        if (sb.length() > 100) {
          sb.append(", ...");
          break;
        }
        separator = ", ";
      }
      sb.append("}");
      return sb.toString();
    }

  }

  public static class EnforcerList extends ArrayList {

    @Override
    @SuppressWarnings("unchecked")
    public boolean add(Object o) {
      try {
        return super.add(o);
      } finally {
        checkIfLengthExceededForObjectRead(this);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("[");
      String separator = "";
      for (Object value : this) {
        sb.append(separator).append(value);
        if (sb.length() > 100) {
          sb.append(", ...");
          break;
        }
        separator = ", ";
      }
      sb.append("]");
      return sb.toString();
    }
  }

  private static class MapDeserializer extends JsonDeserializer<Map> {

    @Override
    public Map deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
      return jp.readValueAs(EnforcerMap.class);
    }

  }

  private static class ListDeserializer extends JsonDeserializer<List> {

    @Override
    public List deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
      return jp.readValueAs(EnforcerList.class);
    }

  }

  private static final ThreadLocal<OverrunStreamingJsonParser> TL = new ThreadLocal<>();

  private final OverrunReader countingReader;
  private final int maxObjectLen;
  private long startOffset;
  private long limitOffset;
  private boolean overrun;

  public OverrunStreamingJsonParser(CountingReader reader, Mode mode, int maxObjectLen) throws IOException {
    this(reader, 0, mode, maxObjectLen);
  }

  public OverrunStreamingJsonParser(CountingReader reader, long initialPosition, Mode mode, int maxObjectLen)
      throws IOException {
    this(new OverrunReader(reader, OverrunReader.getDefaultReadLimit(), false, false), initialPosition, mode, maxObjectLen);
  }

  public OverrunStreamingJsonParser(OverrunReader reader, long initialPosition, Mode mode, int maxObjectLen)
      throws IOException {
    super(reader, initialPosition, mode);
    countingReader = (OverrunReader) getReader();
    countingReader.setEnabled(true);
    this.maxObjectLen = maxObjectLen;
  }

  @Override
  protected void fastForwardLeaseReader() {
    ((CountingReader) getReader()).resetCount();
  }

  @Override
  protected ObjectMapper getObjectMapper() {
    return OBJECT_MAPPER;
  }

  @Override
  protected Object readObjectFromArray() throws IOException {
    Utils.checkState(!overrun, "The underlying input stream had an overrun, the parser is not usable anymore");
    countingReader.resetCount();
    startOffset = getJsonParser().getCurrentLocation().getCharOffset();
    limitOffset = startOffset + maxObjectLen;
    try {
      TL.set(this);
      return super.readObjectFromArray();
    } catch (Exception ex) {
      ObjectLengthException olex = ExceptionUtils.findSpecificCause(ex, ObjectLengthException.class);
      if (olex != null) {
        JsonParser parser = getJsonParser();
        JsonToken token = parser.getCurrentToken();
        if (token == null) {
          token = parser.nextToken();
        }
        while (token != null && parser.getParsingContext() != getRootContext()) {
          token = parser.nextToken();
        }
        throw olex;
      } else {
        OverrunException oex = ExceptionUtils.findSpecificCause(ex, OverrunException.class);
        if (oex != null) {
          overrun = true;
          throw oex;
        }
        throw ex;
      }
    } finally {
      TL.remove();
    }
  }

  @Override
  protected Object readObjectFromStream() throws IOException {
    Utils.checkState(!overrun, "The underlying input stream had an overrun, the parser is not usable anymore");
    countingReader.resetCount();
    limitOffset = getJsonParser().getCurrentLocation().getCharOffset() + maxObjectLen;
    try {
      TL.set(this);
      return super.readObjectFromStream();
    } catch (Exception ex) {
      ObjectLengthException olex = ExceptionUtils.findSpecificCause(ex, ObjectLengthException.class);
      if (olex != null) {
        fastForwardToNextRootObject();
        throw olex;
      } else {
        OverrunException oex = ExceptionUtils.findSpecificCause(ex, OverrunException.class);
        if (oex != null) {
          overrun = true;
          throw oex;
        }
        throw ex;
      }
    } finally {
      TL.remove();
    }
  }

  private static void checkIfLengthExceededForObjectRead(Object json) {
    OverrunStreamingJsonParser enforcer = TL.get();
    if (enforcer.maxObjectLen > -1) {
      if (enforcer.getJsonParser().getCurrentLocation().getCharOffset() > enforcer.limitOffset) {
        ExceptionUtils.throwUndeclared(new ObjectLengthException(Utils.format(
            "JSON Object at offset '{}' exceeds max length '{}'", enforcer.startOffset, enforcer.maxObjectLen),
                                                                 enforcer.startOffset));
      }
    }
  }

}
