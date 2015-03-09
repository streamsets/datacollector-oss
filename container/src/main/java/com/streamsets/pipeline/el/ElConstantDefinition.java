/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

public class ElConstantDefinition {

  private final String name;
  private final String description;
  private final String returnType;

  public ElConstantDefinition(String name, String description, String returnType) {
    this.name = name;
    this.description = description;
    this.returnType = returnType;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getReturnType() {
    return returnType;
  }
}
