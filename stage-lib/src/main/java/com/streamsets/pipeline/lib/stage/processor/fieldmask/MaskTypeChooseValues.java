/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldmask;

import com.streamsets.pipeline.api.ChooserValues;

import java.util.ArrayList;
import java.util.List;

public class MaskTypeChooseValues implements ChooserValues {
  @Override
  public List<String> getValues() {
    List<String> values = new ArrayList<>();
    for (FieldMaskProcessor.Type type : FieldMaskProcessor.Type.values()) {
      values.add(type.toString());
    }
    return values;
  }

  @Override
  public List<String> getLabels() {
    List<String> labels = new ArrayList<>();
    for (FieldMaskProcessor.Type type : FieldMaskProcessor.Type.values()) {
      labels.add(type.name());
    }
    return labels;
  }
}
