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
