/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.el;

import java.lang.reflect.Field;

public class ElConstantDefinition {
  private final String index;
  private final String name;
  private final String description;
  private final String returnType;

  public ElConstantDefinition(String index, String name, String description, String returnType) {
    this.index = index;
    this.name = name;
    this.description = description;
    this.returnType = returnType;
  }

  public String getIndex() {
    return index;
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

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (! (obj instanceof ElConstantDefinition)) {
      return false;
    }
    return toString().equals(obj.toString());
  }

  public String toString() {
    return name;
  }

}
