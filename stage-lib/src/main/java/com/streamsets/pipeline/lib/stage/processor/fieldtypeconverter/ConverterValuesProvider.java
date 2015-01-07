package com.streamsets.pipeline.lib.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.ChooserValues;

import java.util.ArrayList;
import java.util.List;

public class ConverterValuesProvider implements ChooserValues {
    @Override
    public List<String> getValues() {
      List<String> values = new ArrayList<>();
      for (FieldTypeConverterProcessor.FieldType type : FieldTypeConverterProcessor.FieldType.values()) {
        values.add(type.toString());
      }
      return values;
    }

    @Override
    public List<String> getLabels() {
      List<String> labels = new ArrayList<>();
      for (FieldTypeConverterProcessor.FieldType type : FieldTypeConverterProcessor.FieldType.values()) {
        labels.add(type.name());
      }
      return labels;
    }
  }