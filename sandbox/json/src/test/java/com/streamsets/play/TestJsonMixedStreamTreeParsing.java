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
package com.streamsets.play;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestJsonMixedStreamTreeParsing {

  public static class JsonParserObjectLengthEnforcer {
    private static final ThreadLocal<JsonParserObjectLengthEnforcer> TL =
        new ThreadLocal<JsonParserObjectLengthEnforcer>();

    private final JsonParser parser;
    private final int maxLen;
    private long limit;

    public JsonParserObjectLengthEnforcer(JsonParser parser, int maxObjectLen) {
      this.parser = parser;
      this.maxLen = maxObjectLen;
    }

    public Map parseMap() throws IOException {
      limit = parser.getCurrentLocation().getByteOffset() + maxLen;
      try {
        TL.set(this);
        return parser.readValueAs(Map.class);
      } finally {
        TL.remove();
      }
    }

    public List parseList() throws IOException {
      limit = parser.getCurrentLocation().getByteOffset() + maxLen;
      try {
        TL.set(this);
        return parser.readValueAs(List.class);
      } finally {
        TL.remove();
      }
    }

    public static class ObjectLengthException extends RuntimeException {
      public ObjectLengthException(String message) {
        super(message);
      }
    }

    public static void check(Object json) {
      JsonParserObjectLengthEnforcer enforcer = TL.get();
      if (enforcer.parser.getCurrentLocation().getByteOffset() > enforcer.limit) {
        throw new ObjectLengthException("Exceeded the Object max length [" + json + "]");
      }
    }

    public static class EnforcerMap extends LinkedHashMap {

      @Override
      public Object put(Object key, Object value) {
        JsonParserObjectLengthEnforcer.check(this);
        return super.put(key, value);
      }
    }

    public static class EnforcerList extends ArrayList {

      @Override
      public boolean add(Object o) {
        JsonParserObjectLengthEnforcer.check(this);
        return super.add(o);
      }
    }

    public static class EnforcerMapDeserializer extends JsonDeserializer<Map> {

      @Override
      public Map deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        return jp.readValueAs(EnforcerMap.class);
      }

    }

    public static class EnforcerListDeserializer extends JsonDeserializer<List> {

      @Override
      public List deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        return jp.readValueAs(EnforcerList.class);
      }

    }

  }



  @Test
  public void test() throws Exception {
    InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("input.json");
    ObjectMapper om = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(Map.class, new JsonParserObjectLengthEnforcer.EnforcerMapDeserializer());
    module.addDeserializer(List.class, new JsonParserObjectLengthEnforcer.EnforcerListDeserializer());
    om.registerModule(module);
    JsonParser parser = om.getFactory().createParser(in);

    JsonToken token = parser.nextToken();
    JsonStreamContext rootContext = parser.getParsingContext();
    if (token == JsonToken.START_ARRAY) {
      token = parser.nextToken();
      while (token != null && token != JsonToken.END_ARRAY) {
        try {
          JsonParserObjectLengthEnforcer parserE = new JsonParserObjectLengthEnforcer(parser, 100);
          Map map = parserE.parseMap();
          System.out.println(map);
          token = parser.nextToken();
        } catch (JsonMappingException ex) {
          if (ex.getCause() instanceof JsonParserObjectLengthEnforcer.ObjectLengthException) {
            System.out.println("Discarding element, " + ex.getCause().getMessage());
            token = parser.getCurrentToken();
            while (token != null && parser.getParsingContext() != rootContext) {
              token = parser.nextToken();
            }
            if (token != null) {
              token = parser.nextToken();
            }
          } else {
            throw ex;
          }
        }
      }
    }
  }

}
