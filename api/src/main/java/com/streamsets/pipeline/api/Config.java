/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

import java.io.Serializable;

import com.streamsets.pipeline.api.impl.Utils;

public class Config implements Serializable{
  private final String name;
  private final Object value;

  public Config(String name, Object value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public String toString() {
    return Utils.format("Config[name='{}' value='{}']", getName(), getValue());
  }

}
