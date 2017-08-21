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

import com.streamsets.pipeline.api.impl.Utils;

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
