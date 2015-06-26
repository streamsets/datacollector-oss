/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import java.lang.reflect.Method;
import java.util.List;

public class ElFunctionDefinition {
  private final String index;
  private final String name;
  private final String description;
  private final String group;
  private final String returnType;
  private final List<ElFunctionArgumentDefinition> elFunctionArgumentDefinition;

  public ElFunctionDefinition(String index, String group, String name, String description,
      List<ElFunctionArgumentDefinition> elFunctionArgumentDefinition, String returnType) {
    this.index = index;
    this.name = name;
    this.description = description;
    this.group = group;
    this.returnType = returnType;
    this.elFunctionArgumentDefinition = elFunctionArgumentDefinition;
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

  public String getGroup() {
    return group;
  }

  public String getReturnType() {
    return returnType;
  }

  public List<ElFunctionArgumentDefinition> getElFunctionArgumentDefinition() {
    return elFunctionArgumentDefinition;
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
    if (! (obj instanceof ElFunctionDefinition)) {
      return false;
    }
    return toString().equals(obj.toString());
  }

  public String toString() {
    return name;
  }

}
