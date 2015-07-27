/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.el;

import com.streamsets.pipeline.api.impl.Utils;

public class ElFunctionArgumentDefinition {

  private final String name;
  private final String type;

  public ElFunctionArgumentDefinition(String name, String type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public String toString() {
    return Utils.format("{}:{}", name, type);
  }
}
