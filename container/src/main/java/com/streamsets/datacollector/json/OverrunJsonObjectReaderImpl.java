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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.streamsets.pipeline.api.ext.io.CountingReader;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.ext.io.ObjectLengthException;
import com.streamsets.pipeline.api.ext.io.OverrunException;
import com.streamsets.pipeline.lib.util.ExceptionUtils;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class OverrunJsonObjectReaderImpl extends JsonObjectReaderImpl {
  private static final ObjectMapper DEFAULT_OVERRUN_OBJECT_MAPPER = new ObjectMapper();
  private static final ThreadLocal<OverrunJsonObjectReaderImpl> TL = new ThreadLocal<>();

  private final OverrunReader countingReader;
  private final int maxObjectLen;
  private long startOffset;
  private long limitOffset;
  private boolean overrun;

  static {
    SimpleModule module = new SimpleModule();
    module.addDeserializer(Map.class, new MapDeserializer());
    module.addDeserializer(List.class, new ListDeserializer());
    DEFAULT_OVERRUN_OBJECT_MAPPER.registerModule(module);
    DEFAULT_OVERRUN_OBJECT_MAPPER.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    DEFAULT_OVERRUN_OBJECT_MAPPER.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
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

  public OverrunJsonObjectReaderImpl(Reader reader, long initialPosition, int maxObjectLen, Mode mode) throws IOException {
    this(reader, initialPosition, maxObjectLen, mode, Object.class);
  }

  public OverrunJsonObjectReaderImpl(Reader reader, long initialPosition, int maxObjectLen, Mode mode, Class<?> objectClass) throws IOException {
    this(reader, initialPosition, maxObjectLen, mode, objectClass, DEFAULT_OVERRUN_OBJECT_MAPPER);
  }

  public OverrunJsonObjectReaderImpl(Reader reader, long initialPosition, int maxObjectLen, Mode mode, Class<?> objectClass, ObjectMapper objectMapper) throws IOException {
    super(reader, initialPosition, mode, objectClass, objectMapper);
    countingReader = (OverrunReader) getReader();
    countingReader.setEnabled(true);
    this.maxObjectLen = maxObjectLen;
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
    OverrunJsonObjectReaderImpl enforcer = TL.get();
    if (checkNotNull(enforcer, "Enforcer was null").maxObjectLen > -1) {
      if (enforcer.getJsonParser().getCurrentLocation().getCharOffset() > enforcer.limitOffset) {
        ExceptionUtils.throwUndeclared(new ObjectLengthException(Utils.format(
            "JSON Object at offset '{}' exceeds max length '{}'", enforcer.startOffset, enforcer.maxObjectLen),
            enforcer.startOffset));
      }
    }
  }

  @Override
  protected void fastForwardLeaseReader() {
    ((CountingReader) getReader()).resetCount();
  }
}
