/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.devtest;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ChooserValues;

import java.util.ArrayList;
import java.util.List;

public class RootTypeChooserValueProvider implements ChooserValues {

  @Override
  public String getResourceBundle() {
    return null;
  }

  @Override
  public List<String> getValues() {
    List<String> values = new ArrayList<>();
    for (RandomDataGeneratorSource.RootType type : RandomDataGeneratorSource.RootType.values()) {
      values.add(type.toString());
    }
    return values;
  }

  @Override
  public List<String> getLabels() {
    List<String> labels = new ArrayList<>();
    for (RandomDataGeneratorSource.RootType type : RandomDataGeneratorSource.RootType.values()) {
      labels.add(type.toString());
    }
    return labels;
  }
}
