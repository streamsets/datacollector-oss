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

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.streamsets.datacollector.restapi.rbean.lang.REnum;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.function.Function;

public class REnumJacksonDeserializer extends AbstractRValueJacksonDeserializer<Enum, REnum<Enum>> {

  @SuppressWarnings("unchecked")
  public REnumJacksonDeserializer() {
    super((Class<REnum<Enum>>) (Class)REnum.class);
  }

  @Override
  protected Function<JsonNode, Enum> getConvertIfNotNull() {
    return null; //not used for enums as we have t infer the Enum class first
  }

  Class<Enum> getEnumClass(DeserializationContext deserializationContext) throws IOException {
    String propertyName = null;
    try {
      propertyName = deserializationContext.getParser().getCurrentName();
      return (Class<Enum>) (
          (ParameterizedType) getInheritedFieldByName(
              deserializationContext.getParser().getCurrentValue().getClass(),
              propertyName
          ).getGenericType()
      ).getActualTypeArguments()[0];
    } catch (Exception ex) {
      throw new IOException(Utils.format(
          "Could not Determine Enum for REnum<> '{}': {}",
          propertyName,
          ex
      ));
    }
  }

  private Field getInheritedFieldByName(Class c, String propertyName) throws Exception {
    Exception firstException = null;
    Field field = null;
    while (field == null && c != null) {
      try {
        field = c.getDeclaredField(propertyName);
      } catch (NoSuchFieldException e) {
        // Continue
        if (firstException == null) {
          firstException = e;
        }
      }
      c = c.getSuperclass();
    }

    if (field == null) {
      throw firstException;
    }
    return field;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Enum convertIfNotNull(JsonNode container, DeserializationContext deserializationContext, Function<JsonNode, Enum> converter) throws
      IOException {
    Enum value = null;
    JsonNode jsonValue = getNodeValue(container, deserializationContext);
    if (!jsonValue.isNull()) {
      Class enumClass = getEnumClass(deserializationContext);
      try {
        value = Enum.valueOf(enumClass, jsonValue.asText());
      } catch (IllegalArgumentException ex) {
        throw new IOException(Utils.format(
            "Could not parse '{}' as a '{}' enum",
            jsonValue.asText(),
            enumClass.getName()
        ));
      }
    }
    return value;
  }

}
