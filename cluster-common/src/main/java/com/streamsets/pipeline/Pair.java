/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import java.util.Map;

/**
 * Wrapper around the message received from spark
 */
public class Pair implements Map.Entry {

  private Object first;
  private Object second;


  public Pair(Object first, Object second) {
    this.first = first;
    this.second = second;
  }
  public Object getFirst() {
    return first;
  }

  public Object getSecond()  {
    return second;
  }


  @Override
  public Object getKey() {
    return first;
  }

  @Override
  public Object getValue() {
    return second;
  }

  @Override
  public Object setValue(Object value) {
    throw new UnsupportedOperationException();
  }
}
