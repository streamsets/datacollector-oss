/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.record;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.restapi.bean.FieldJson;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
