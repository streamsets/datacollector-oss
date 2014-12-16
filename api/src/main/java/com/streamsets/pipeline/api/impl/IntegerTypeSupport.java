/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import java.math.BigDecimal;

public class IntegerTypeSupport extends TypeSupport<Integer> {

  @Override
  public Integer convert(Object value) {
    if (value instanceof Integer) {
      return (Integer) value;
    }
    if (value instanceof String) {
      return Integer.parseInt((String) value);
    }
    if (value instanceof Short) {
      return ((Short)value).intValue();
    }
    if (value instanceof Long) {
      return ((Long)value).intValue();
    }
    if (value instanceof Byte) {
      return ((Byte)value).intValue();
    }
    if (value instanceof Float) {
      return ((Float)value).intValue();
    }
    if (value instanceof Double) {
      return ((Double)value).intValue();
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal)value).intValue();
    }
    throw new IllegalArgumentException(Utils.format("Cannot convert {} '{}' to a int",
                                                    value.getClass().getSimpleName(), value));
  }

}
