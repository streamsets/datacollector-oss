/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.base.Errors;

// we are making a Field for byte[]'s to have 'immutable' values by cloning on create/get/clone
public class ByteArrayTypeSupport extends TypeSupport<byte[]> {

  @Override
  public byte[] convert(Object value) {
    if (value instanceof byte[]) {
      return (byte[])value;
    }
    throw new IllegalArgumentException(Utils.format(Errors.API_02.getMessage(),
                                                    value.getClass().getSimpleName(), value));
  }

  @Override
  public Object convert(Object value, TypeSupport targetTypeSupport) {
    if (targetTypeSupport instanceof ByteArrayTypeSupport) {
      return value;
    } else {
      throw new IllegalArgumentException(Utils.format(Errors.API_03.getMessage(), targetTypeSupport));
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

  @Override
  public boolean equals(Object value1, Object value2) {
    return (value1 == value2) || (value1 != null && value2 != null && arrayEquals((byte[])value1, (byte[])value2));
  }

  private boolean arrayEquals(byte[] arr1, byte[] arr2) {
    boolean eq = false;
    if (arr1.length == arr2.length) {
      eq = true;
      for (int i = 0; eq && i < arr1.length; i++) {
        if (arr1[i] != arr2[i]) {
          eq = false;
        }
      }
    }
    return eq;
  }

}
