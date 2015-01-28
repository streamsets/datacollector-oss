/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.base.BaseError;

// we are making a Field for byte[]'s to have 'immutable' values by cloning on create/get/clone
public class ByteArrayTypeSupport extends TypeSupport<byte[]> {

  @Override
  public byte[] convert(Object value) {
    if (value instanceof byte[]) {
      return (byte[])value;
    }
    throw new IllegalArgumentException(Utils.format(BaseError.BASE_0003.getMessage(),
                                                    value.getClass().getSimpleName(), value));
  }

  @Override
  public Object convert(Object value, TypeSupport targetTypeSupport) {
    if (targetTypeSupport instanceof ByteArrayTypeSupport) {
      return value;
    } else {
      throw new IllegalArgumentException(Utils.format(BaseError.BASE_0004.getMessage(), targetTypeSupport));
    }
  }

  @Override
  public Object create(Object value) {
    return clone(value);
  }

  @Override
  public Object get(Object value) {
    return clone(value);
  }

  @Override
  public Object clone(Object value) {
    return ((byte[])value).clone();
  }

}
