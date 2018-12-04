/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.Field;

import java.math.BigDecimal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class FieldUtils {
  public static Field.Type getTypeFromObject(Object result) {
    if(result instanceof Double) {
      return Field.Type.DOUBLE;
    } else if(result instanceof Long) {
      return Field.Type.LONG;
    } else if(result instanceof BigDecimal) {
      return Field.Type.DECIMAL;
    } else if(result instanceof Date) {
      //This can only happen in ${time:now()}
      return Field.Type.DATETIME;
      //For all the timeEL, we currently return String so we are safe.
    } else if(result instanceof Short) {
      return Field.Type.SHORT;
    } else if(result instanceof Boolean) {
      return Field.Type.BOOLEAN;
    } else if(result instanceof Byte) {
      return Field.Type.BYTE;
    } else if(result instanceof byte[]) {
      return Field.Type.BYTE_ARRAY;
    } else if(result instanceof Character) {
      return Field.Type.CHAR;
    } else if(result instanceof Float) {
      return Field.Type.FLOAT;
    } else if(result instanceof Integer) {
      return Field.Type.INTEGER;
    } else if(result instanceof String) {
      return Field.Type.STRING;
    } else if(result instanceof LinkedHashMap) {
      return Field.Type.LIST_MAP;
    } else if(result instanceof Map) {
      return Field.Type.MAP;
    } else if(result instanceof List) {
      return Field.Type.LIST;
    }
    return Field.Type.STRING;
  }
}
