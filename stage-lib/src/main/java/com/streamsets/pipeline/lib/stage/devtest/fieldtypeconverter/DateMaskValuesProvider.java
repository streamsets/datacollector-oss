package com.streamsets.pipeline.lib.stage.devtest.fieldtypeconverter;

import com.streamsets.pipeline.api.ChooserValues;

import java.util.ArrayList;
import java.util.List;

public class DateMaskValuesProvider implements ChooserValues {
    @Override
    public List<String> getValues() {
      List<String> values = new ArrayList<>();
      for (DateMask dateMask : DateMask.values()) {
        values.add(dateMask.toString());
      }
      return values;
    }

    @Override
    public List<String> getLabels() {
      List<String> labels = new ArrayList<>();
      for (DateMask dateMask : DateMask.values()) {
        labels.add(dateMask.name());
      }
      return labels;
    }
  }