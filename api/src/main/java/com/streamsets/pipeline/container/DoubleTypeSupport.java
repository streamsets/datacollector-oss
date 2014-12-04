/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import java.math.BigDecimal;

public class DoubleTypeSupport extends TypeSupport<Double> {

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
    throw new IllegalArgumentException(Utils.format("Cannot convert {} '{}' to a double",
                                                    value.getClass().getSimpleName(), value));
  }

}
