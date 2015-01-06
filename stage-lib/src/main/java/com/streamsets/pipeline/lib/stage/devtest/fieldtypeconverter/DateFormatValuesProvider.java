package com.streamsets.pipeline.lib.stage.devtest.fieldtypeconverter;

import com.streamsets.pipeline.api.ChooserValues;

import java.util.ArrayList;
import java.util.List;

public class DateFormatValuesProvider implements ChooserValues {
    @Override
    public List<String> getValues() {
      List<String> values = new ArrayList<>();
      for (DateFormat dateFormat : DateFormat.values()) {
        values.add(dateFormat.toString());
      }
      return values;
    }

    @Override
    public List<String> getLabels() {
      List<String> labels = new ArrayList<>();
      for (DateFormat dateFormat : DateFormat.values()) {
        labels.add(dateFormat.name());
      }
      return labels;
    }
  }