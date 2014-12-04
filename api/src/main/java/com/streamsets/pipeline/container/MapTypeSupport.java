/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import com.streamsets.pipeline.api.Field;

import java.text.ParseException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class MapTypeSupport extends TypeSupport<Map> {

  @Override
  public Map convert(Object value) {
    if (value instanceof Map) {
      return (Map) value;
    }
    throw new IllegalArgumentException(Utils.format("Cannot convert {} '{}' to a Map",
                                                    value.getClass().getSimpleName(), value));
  }

  @Override
  public Object convert(Object value, TypeSupport targetTypeSupport) {
    if (targetTypeSupport instanceof MapTypeSupport) {
      return value;
    } else {
      throw new IllegalArgumentException(Utils.format("Cannot convert Map to other type, {}", targetTypeSupport));
    }
  }


  @Override
  @SuppressWarnings("unchecked")
  public Object clone(Object value) {
    Map map = null;
    if (value != null) {
      map = deepCopy((Map<String, Field>)value);
    }
    return map;
  }

  private Map<String, Field> deepCopy(Map<String, Field> map) {
    Map<String, Field> copy = new LinkedHashMap<>();
    for (Map.Entry<String, Field> entry : map.entrySet()) {
      String name = entry.getKey();
      Utils.checkNotNull(name, "Map cannot have null keys");
      Utils.checkNotNull(entry.getValue(), Utils.format("Map cannot have null values, key '{}'", name));
      Utils.checkArgument(TextUtils.isValidName(name), Utils.format("Invalid key name '{}', must be '{}''", name,
                                                                    TextUtils.VALID_NAME));
      copy.put(entry.getKey(), entry.getValue().clone());
    }
    return copy;
  }

}
