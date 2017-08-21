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
package com.streamsets.datacollector.record;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.streamsets.datacollector.restapi.bean.FieldJson;
import com.streamsets.datacollector.util.EscapeUtil;
import com.streamsets.pipeline.api.Field;

import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FieldDeserializer extends JsonDeserializer<FieldJson> {

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
      Map<String, String> attributes = (Map<String, String>) map.get("attributes");
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
              String path = (String)element.get("sqpath");
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
      field = Field.create(type, value, attributes);
    }
    return field;
  }

}
