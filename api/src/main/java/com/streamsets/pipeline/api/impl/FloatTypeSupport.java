/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.base.BaseError;

import java.math.BigDecimal;

public class FloatTypeSupport extends TypeSupport<Float> {

  @Override
  public Float convert(Object value) {
    if (value instanceof Float) {
      return (Float) value;
    }
    if (value instanceof String) {
      return Float.parseFloat((String) value);
    }
    if (value instanceof Short) {
      return ((Short)value).floatValue();
    }
    if (value instanceof Integer) {
      return ((Integer)value).floatValue();
    }
    if (value instanceof Byte) {
      return ((Byte)value).floatValue();
    }
    if (value instanceof Long) {
      return ((Long)value).floatValue();
    }
    if (value instanceof Double) {
      return ((Double)value).floatValue();
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal)value).floatValue();
    }
    throw new IllegalArgumentException(Utils.format(BaseError.BASE_0011.getMessage(),
                                                    value.getClass().getSimpleName(), value));
  }

}
