/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class BaseEnumChooserValues<T extends Enum> implements ChooserValues {
  private String resourceBundle;
  private List<String> values;
  private List<String> labels;

  @SuppressWarnings("unchecked")
  public BaseEnumChooserValues(Class<? extends Enum> klass) {
    this((T[])klass.getEnumConstants());
  }

  @SuppressWarnings("unchecked")
  public BaseEnumChooserValues(T ... enums) {
    Utils.checkNotNull(enums, "enums");
    Utils.checkArgument(enums.length > 0, "array enum cannot have zero elements");
    resourceBundle = enums[0].getClass().getName() + "-bundle";
    boolean isEnumWithLabels = enums[0] instanceof Label;
    values = new ArrayList<>(enums.length);
    labels = new ArrayList<>(enums.length);
    for (T e : enums) {
      String value = e.name();
      values.add(value);
      String label = (isEnumWithLabels) ? ((Label)e).getLabel() : value;
      labels.add(label);
    }
    values = Collections.unmodifiableList(values);
    labels = Collections.unmodifiableList(labels);
  }

  @Override
  public String getResourceBundle() {
    return resourceBundle;
  }

  @Override
  public List<String> getValues() {
    return values;
  }

  @Override
  public List<String> getLabels() {
    return labels;
  }
}
