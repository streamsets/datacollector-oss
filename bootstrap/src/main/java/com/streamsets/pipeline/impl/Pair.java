package com.streamsets.pipeline.impl;

import java.util.Map;

public class Pair implements Map.Entry {

  private Object key;
  private Object value;


  public Pair(Object key, Object value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public Object getKey() {
    return key;
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public Object setValue(Object value) {
    throw new UnsupportedOperationException();
  }
}
