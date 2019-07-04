/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.el;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.pipeline.api.impl.Utils;

import java.lang.reflect.Method;
import java.util.List;

public class ElFunctionDefinition {
  private final String index;
  private final String name;
  private final String description;
  private final String group;
  private final String returnType;
  private final List<ElFunctionArgumentDefinition> elFunctionArgumentDefinition;
  public final Method method;
  public boolean implicitOnly;

  public ElFunctionDefinition(
      String index,
      String group,
      String name,
      String description,
      boolean implicitOnly,
      List<ElFunctionArgumentDefinition> elFunctionArgumentDefinition,
      String returnType,
      Method method
  ) {
    this.index = index;
    this.name = name;
    this.description = description;
    this.group = group;
    this.returnType = returnType;
    this.elFunctionArgumentDefinition = elFunctionArgumentDefinition;
    this.method = method;
    this.implicitOnly = implicitOnly;
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

  @JsonIgnore
  public Method getMethod() {
    return method;
  }

  public boolean isImplicitOnly() {
    return implicitOnly;
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
    return Utils.format("ELFunctionDefinition[name='{}', arguments='{}' returnType='{}']", name,
                        elFunctionArgumentDefinition, returnType);
  }

}
