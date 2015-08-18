/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.base.Errors;

import java.util.LinkedHashMap;
import java.util.Map;

public class MapTypeSupport extends TypeSupport<Map> {

  @Override
  public Map convert(Object value) {
    if (value instanceof Map) {
      return (Map) value;
    }
    throw new IllegalArgumentException(Utils.format(Errors.API_15.getMessage(),
                                                    value.getClass().getSimpleName(), value));
  }

  @Override
  public Object convert(Object value, TypeSupport targetTypeSupport) {
    if (targetTypeSupport instanceof MapTypeSupport || targetTypeSupport instanceof ListMapTypeSupport) {
      return value;
    } else {
      throw new IllegalArgumentException(Utils.format(Errors.API_16.getMessage(), targetTypeSupport));
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
      Utils.checkNotNull(entry.getValue(), Utils.formatL("Map cannot have null values, key '{}'", name));
      copy.put(entry.getKey(), entry.getValue().clone());
    }
    return copy;
  }

  @Override
  public Object create(Object value) {
    return clone(value);
  }

}
