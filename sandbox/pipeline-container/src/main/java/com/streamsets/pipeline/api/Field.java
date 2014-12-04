/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

public class Field {
  private Object value;

  public Field(Object value) {
    this.value = value;
  }
  public Object value() {
    return value;
  }

}
