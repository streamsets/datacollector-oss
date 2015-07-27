/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.el;

import com.streamsets.pipeline.api.impl.Utils;

import java.lang.reflect.Field;

public class ElConstantDefinition {
  private final String index;
  private final String name;
  private final String description;
  private final String returnType;
  private final Object value;

  public ElConstantDefinition(String index, String name, String description, String returnType, Object value) {
    this.index = index;
    this.name = name;
    this.description = description;
    this.returnType = returnType;
    this.value = value;
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

  public Object getValue() {
    return value;
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
    return Utils.format("ELConstantDefinition[name='{}', type='{}']", name, returnType);
  }

}
