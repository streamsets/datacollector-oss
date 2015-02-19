/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.devtest;

import com.streamsets.pipeline.api.ChooserValues;

import java.util.ArrayList;
import java.util.List;

public class TypeChooserValueProvider implements ChooserValues {
  @Override
  public List<String> getValues() {
    List<String> values = new ArrayList<>();
    for (RandomDataGenerator.Type type : RandomDataGenerator.Type.values()) {
      values.add(type.toString());
    }
    return values;
  }

  @Override
  public List<String> getLabels() {
    List<String> labels = new ArrayList<>();
    for (RandomDataGenerator.Type type : RandomDataGenerator.Type.values()) {
      labels.add(type.toString());
    }
    return labels;
  }
}
