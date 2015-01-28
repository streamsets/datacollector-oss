/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.base.BaseError;

import java.math.BigDecimal;

public class ShortTypeSupport extends TypeSupport<Short> {

  @Override
  public Short convert(Object value) {
    if (value instanceof Short) {
      return (Short) value;
    }
    if (value instanceof String) {
      return Short.parseShort((String) value);
    }
    if (value instanceof Integer) {
      return ((Integer)value).shortValue();
    }
    if (value instanceof Long) {
      return ((Long)value).shortValue();
    }
    if (value instanceof Byte) {
      return ((Byte)value).shortValue();
    }
    if (value instanceof Float) {
      return ((Float)value).shortValue();
    }
    if (value instanceof Double) {
      return ((Double)value).shortValue();
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal)value).shortValue();
    }
    throw new IllegalArgumentException(Utils.format(BaseError.BASE_0018.getMessage(),
                                                    value.getClass().getSimpleName(), value));
  }

}
