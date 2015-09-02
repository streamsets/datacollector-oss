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

public class DecimalTypeSupport extends TypeSupport<BigDecimal> {

  @Override
  public BigDecimal convert(Object value) {
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }
    if (value instanceof String) {
      return new BigDecimal((String) value);
    }
    if (value instanceof Short) {
      return new BigDecimal((Short)value);
    }
    if (value instanceof Integer) {
      return new BigDecimal((Integer)value);
    }
    if (value instanceof Long) {
      return new BigDecimal((Long)value);
    }
    if (value instanceof Byte) {
      return new BigDecimal((Byte)value);
    }
    if (value instanceof Float) {
      return new BigDecimal((Float)value);
    }
    if (value instanceof Double) {
      return new BigDecimal((Double)value);
    }
    if (value instanceof Number) {
      //http://stackoverflow.com/questions/16216248/convert-java-number-to-bigdecimal-best-way
      return new BigDecimal(value.toString());
    }
    throw new IllegalArgumentException(Utils.format(Errors.API_08.getMessage(),
                                                    value.getClass().getSimpleName(), value));
  }

}
