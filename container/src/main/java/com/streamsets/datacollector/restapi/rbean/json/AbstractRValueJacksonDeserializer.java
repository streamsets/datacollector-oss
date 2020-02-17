/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.restapi.rbean.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.streamsets.datacollector.restapi.rbean.lang.RValue;

import java.io.IOException;
import java.util.function.Function;

public abstract class AbstractRValueJacksonDeserializer<V, R extends RValue<R, V>> extends StdDeserializer<R> {

  public AbstractRValueJacksonDeserializer(Class<R> vc) {
    super(vc);
  }

  @Override
  public R deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    JsonNode jsonContainer = jsonParser.getCodec().readTree(jsonParser);
    return createRValue(jsonContainer, deserializationContext).setScrubbed(isScrubbed(jsonContainer, deserializationContext));
  }

  @Override
  public R getNullValue(DeserializationContext ctxt) throws JsonMappingException {
    return createRValue();
  }

  protected boolean isScrubbed(JsonNode jsonValue, DeserializationContext deserializationContext) {
    boolean scrubbed = jsonValue.isArray() &&
        (jsonValue).size() == 1 &&
        RValueJacksonSerializer.SCRUBBED_ELEMENT.equals((jsonValue).get(0).asText());
    return scrubbed;
  }

  @SuppressWarnings("unchecked")
  protected R createRValue() {
    try {
      return (R) handledType().getConstructor().newInstance();
    } catch (Exception ex) {
      throw new RuntimeException("Should never happen: " + ex);
    }
  }

  protected abstract Function<JsonNode, V> getConvertIfNotNull();

  protected JsonNode getNodeValue(JsonNode container, DeserializationContext deserializationContext) throws
      IOException {
    return container;
  }

  protected V convertIfNotNull(
      JsonNode container,
      DeserializationContext deserializationContext,
      Function<JsonNode, V> converter
  )
      throws IOException {
    JsonNode jsonValue = getNodeValue(container, deserializationContext);
    return (jsonValue.isNull()) ? null : converter.apply(jsonValue);
  }

  protected R createRValue(JsonNode container, DeserializationContext deserializationContext) throws IOException {
    return createRValue().setValue(convertIfNotNull(container, deserializationContext, getConvertIfNotNull()));
  }

}
