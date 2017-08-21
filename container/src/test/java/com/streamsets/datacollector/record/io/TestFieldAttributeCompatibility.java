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
package com.streamsets.datacollector.record.io;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.streamsets.datacollector.json.ErrorMessageDeserializer;
import com.streamsets.datacollector.json.MetricsObjectMapperFactory;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.FieldJson;
import com.streamsets.datacollector.util.EscapeUtil;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import org.apache.commons.codec.binary.Base64;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TestFieldAttributeCompatibility {

  /**
   * Ensure that records in the new format can be deserialized with the previous code
   */
  @Test
  public void testFieldAttributesForwardCompatible() throws IOException {
    final String withAttrs = "{\"type\":\"STRING\",\"value\":\"value1\"," +
        "\"attributes\":{\"field1_attr1\":\"attrValue1\",\"field1_attr2\":\"attrValue2\"}," +
        "\"sqpath\":\"/field1\",\"dqpath\":\"/field1\"}";

    // in case a null attributes value somehow gets serialized
    final String withNullAttrs = "{\"type\":\"STRING\",\"value\":\"value1\", \"attributes\": null," +
        "\"sqpath\":\"/field1\",\"dqpath\":\"/field1\"}";

    for (String json : new String[]{withAttrs, withNullAttrs}) {
      final ObjectMapper legacyObjectMapper = createNonAttributeObjectMapper();
      final FieldJson field1 = legacyObjectMapper.readValue(json, FieldJson.class);
      assertThat(field1, notNullValue());
      assertThat(field1.getField().getAttributes(), nullValue());
      assertThat(field1.getField().getValue(), Matchers.<Object>equalTo("value1"));
      assertThat(field1.getField().getType(), equalTo(Field.Type.STRING));
    }
  }

  /**
   * Ensure that records in the old format can be deserialized with the new code
   */
  @Test
  public void testFieldAttributesBackwardCompatible() throws IOException {
    final String noAttrs = "{\"type\":\"STRING\",\"value\":\"value1\", \"sqpath\":\"/field1\"," +
        "\"dqpath\":\"/field1\"}";

    final ObjectMapper legacyObjectMapper = ObjectMapperFactory.getOneLine();
    final FieldJson field1 = legacyObjectMapper.readValue(noAttrs, FieldJson.class);
    assertThat(field1, notNullValue());
    assertThat(field1.getField().getAttributes(), nullValue());
    assertThat(field1.getField().getValue(), Matchers.<Object>equalTo("value1"));
    assertThat(field1.getField().getType(), equalTo(Field.Type.STRING));
  }

  /**
   * Essentially the same as {@link com.streamsets.datacollector.json.ObjectMapperFactory#create(boolean)},
   * but using the previous implementation of {@link com.streamsets.datacollector.record.FieldDeserializer}
   *
   * @return the ObjectMapper
   */
  private static ObjectMapper createNonAttributeObjectMapper() {
    ObjectMapper objectMapper = MetricsObjectMapperFactory.create(false);
    SimpleModule module = new SimpleModule();
    module.addDeserializer(FieldJson.class, new PreAttributesFieldDeserializer());
    module.addDeserializer(ErrorMessage.class, new ErrorMessageDeserializer());
    objectMapper.registerModule(module);
    return objectMapper;
  }

  /**
   * The previous implementation (before SDC-4334) of {@link com.streamsets.datacollector.record.FieldDeserializer}
   */
  public static class PreAttributesFieldDeserializer extends JsonDeserializer<FieldJson> {
    @Override
    @SuppressWarnings("unchecked")
    public FieldJson deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
      Field field = parse(jp.readValueAs(Map.class));
      return new FieldJson(field);
    }

    @SuppressWarnings("unchecked")
    private Field parse(Map<String, Object> map) {
      Field field = null;
      if (map != null) {
        Field.Type type = Field.Type.valueOf((String) map.get("type"));
        Object value = map.get("value");
        if (value != null) {
          switch (type) {
            case MAP:
              Map<String, Field> fMap = new HashMap<>();
              for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
                fMap.put(entry.getKey(), parse((Map<String, Object>) entry.getValue()));
              }
              value = fMap;
              break;
            case LIST:
              List<Field> fList = new ArrayList<>();
              for (Map<String, Object> element : (List<Map<String, Object>>) value) {
                fList.add(parse(element));
              }
              value = fList;
              break;
            case LIST_MAP:
              //When converting list to listMap, Key for the listMap is recovered using path attribute
              Map<String, Field> listMap = new LinkedHashMap<>();
              for (Map<String, Object> element : (List<Map<String, Object>>) value) {
                String path = (String) element.get("sqpath");
                listMap.put(EscapeUtil.getLastFieldNameFromPath(path), parse(element));
              }
              value = listMap;
              break;
            case BYTE_ARRAY:
              value = Base64.decodeBase64((String) value);
              break;
            case INTEGER:
              value = Integer.parseInt((String) value);
              break;
            case LONG:
              value = Long.parseLong((String) value);
              break;
            case FLOAT:
              value = Float.parseFloat((String) value);
              break;
            case DOUBLE:
              value = Double.parseDouble((String) value);
              break;
            default:
              break;
          }
        }
        field = Field.create(type, value);
      }
      return field;
    }
  }
}
