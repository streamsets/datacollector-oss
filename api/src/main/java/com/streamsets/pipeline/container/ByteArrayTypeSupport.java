/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

// we are making a Field for byte[]'s to have 'immutable' values by cloning on create/get/clone
public class ByteArrayTypeSupport extends TypeSupport<byte[]> {

  @Override
  public byte[] convert(Object value) {
    if (value instanceof byte[]) {
      return (byte[])value;
    }
    throw new IllegalArgumentException(Utils.format("Cannot convert {} '{}' to a byte[]",
                                                    value.getClass().getSimpleName(), value));
  }

  @Override
  public Object convert(Object value, TypeSupport targetTypeSupport) {
    if (targetTypeSupport instanceof ByteArrayTypeSupport) {
      return value;
    } else {
      throw new IllegalArgumentException(Utils.format("Cannot convert byte[] to other type, {}", targetTypeSupport));
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
