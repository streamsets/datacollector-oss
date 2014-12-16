/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

public abstract class TypeSupport<T> {

  public abstract T convert(Object value);

  public Object convert(Object value, TypeSupport targetTypeSupport) {
    return targetTypeSupport.convert(value);
  }

  public Object create(Object value) {
    return value;
  }

  public Object get(Object value) {
    return value;
  }

  // default implementation assumes value is immutable, no need to clone
  public Object clone(Object value) {
    return value;
  }

}
