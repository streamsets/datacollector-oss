/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
