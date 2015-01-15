/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldfilter;

import com.streamsets.pipeline.api.ChooserValues;

import java.util.ArrayList;
import java.util.List;

public class FilterOperationValues implements ChooserValues {
  @Override
  public List<String> getValues() {
    List<String> values = new ArrayList<>();
    for (FieldFilterProcessor.FilterOperation filterOperation : FieldFilterProcessor.FilterOperation.values()) {
      values.add(filterOperation.toString());
    }
    return values;
  }

  @Override
  public List<String> getLabels() {
    List<String> labels = new ArrayList<>();
    for (FieldFilterProcessor.FilterOperation filterOperation : FieldFilterProcessor.FilterOperation.values()) {
      labels.add(filterOperation.toString());
    }
    return labels;
  }
}
