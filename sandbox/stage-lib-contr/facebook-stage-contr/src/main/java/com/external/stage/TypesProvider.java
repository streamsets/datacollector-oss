/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.external.stage;

import com.streamsets.pipeline.api.ChooserValues;

import java.util.ArrayList;
import java.util.List;

public class TypesProvider implements ChooserValues {

  @Override
  public List<String> getValues() {
    List<String> values = new ArrayList<String>();
    values.add("INT");
    values.add("STRING");
    values.add("DATE");

    return values;
  }

  @Override
  public List<String> getLabels() {
    List<String> labels = new ArrayList<String>();
    labels.add("int_label");
    labels.add("string_label");
    labels.add("date_label");

    return labels;
  }
}
