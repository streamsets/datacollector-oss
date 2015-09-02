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

import com.streamsets.pipeline.api.base.Errors;

import java.math.BigDecimal;

public class BooleanTypeSupport extends TypeSupport<Boolean> {

  @Override
  public Boolean convert(Object value) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    if (value instanceof String) {
      return Boolean.valueOf((String) value);
    }
    if (value instanceof Integer) {
      return ((Integer)value) != 0;
    }
    if (value instanceof Long) {
      return ((Long)value) != 0;
    }
    if (value instanceof Short) {
      return ((Short)value) != 0;
    }
    if (value instanceof Byte) {
      return ((Byte)value) != 0;
    }
    if (value instanceof Float) {
      return ((Float)value) != 0;
    }
    if (value instanceof Double) {
      return ((Double)value) != 0;
    }
    if (value instanceof BigDecimal) {
      return ! value.equals(BigDecimal.ZERO);
    }
    throw new IllegalArgumentException(Utils.format(Errors.API_01.getMessage(),
                                                    value.getClass().getSimpleName(), value));
  }

}
