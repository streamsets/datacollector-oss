/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.base.Errors;

import java.math.BigDecimal;

public class ByteTypeSupport extends TypeSupport<Byte> {

  @Override
  public Byte convert(Object value) {
    if (value instanceof Byte) {
      return (Byte) value;
    }
    if (value instanceof String) {
      return Byte.parseByte((String) value);
    }
    if (value instanceof Integer) {
      return ((Integer)value).byteValue();
    }
    if (value instanceof Long) {
      return ((Long)value).byteValue();
    }
    if (value instanceof Short) {
      return ((Short)value).byteValue();
    }
    if (value instanceof Float) {
      return ((Float)value).byteValue();
    }
    if (value instanceof Double) {
      return ((Double)value).byteValue();
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal)value).byteValue();
    }
    throw new IllegalArgumentException(Utils.format(Errors.API_04.getMessage(),
                                                    value.getClass().getSimpleName(), value));
  }

}
