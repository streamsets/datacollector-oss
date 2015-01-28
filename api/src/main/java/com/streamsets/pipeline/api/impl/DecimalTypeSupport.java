/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.base.BaseError;

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
    throw new IllegalArgumentException(Utils.format(BaseError.BASE_0009.getMessage(),
                                                    value.getClass().getSimpleName(), value));
  }

}
