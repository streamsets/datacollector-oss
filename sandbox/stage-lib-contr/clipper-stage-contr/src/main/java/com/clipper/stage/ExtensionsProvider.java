/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.clipper.stage;

import com.streamsets.pipeline.api.ChooserValues;

import java.util.ArrayList;
import java.util.List;

public class ExtensionsProvider implements ChooserValues {
  @Override
  public List<String> getValues() {
    List<String> values = new ArrayList<>();
    values.add(".txt");
    return values;
  }

  @Override
  public List<String> getLabels() {
    List<String> labels = new ArrayList<>();
    labels.add(".txt");
    return labels;
  }
}
