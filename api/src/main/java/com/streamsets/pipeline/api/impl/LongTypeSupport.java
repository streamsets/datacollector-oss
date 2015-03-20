/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.base.Errors;

import java.math.BigDecimal;

public class LongTypeSupport extends TypeSupport<Long> {

  @Override
  public Long convert(Object value) {
    if (value instanceof Long) {
      return (Long) value;
    }
    if (value instanceof String) {
      return Long.parseLong((String) value);
    }
    if (value instanceof Short) {
      return ((Short)value).longValue();
    }
    if (value instanceof Integer) {
      return ((Integer)value).longValue();
    }
    if (value instanceof Byte) {
      return ((Byte)value).longValue();
    }
    if (value instanceof Float) {
      return ((Float)value).longValue();
    }
    if (value instanceof Double) {
      return ((Double)value).longValue();
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal)value).longValue();
    }
    throw new IllegalArgumentException(Utils.format(Errors.API_14.getMessage(),
                                                    value.getClass().getSimpleName(), value));
  }

}
