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
package com.streamsets.pipeline.api;

import com.streamsets.pipeline.api.impl._ApiUtils;

import java.math.BigDecimal;

class _DoubleTypeSupport extends _TypeSupport<Double> {

  @Override
  public Double convert(Object value) {
    if (value instanceof Double) {
      return (Double) value;
    }
    if (value instanceof String) {
      return Double.parseDouble((String) value);
    }
    if (value instanceof Short) {
      return ((Short)value).doubleValue();
    }
    if (value instanceof Integer) {
      return ((Integer)value).doubleValue();
    }
    if (value instanceof Byte) {
      return ((Byte)value).doubleValue();
    }
    if (value instanceof Long) {
      return ((Long)value).doubleValue();
    }
    if (value instanceof Float) {
      return ((Float)value).doubleValue();
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal)value).doubleValue();
    }
    throw new IllegalArgumentException(_ApiUtils.format("Cannot convert {} '{}' to a double",
                                                        value.getClass().getSimpleName(), value));
  }

}
