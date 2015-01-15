package com.streamsets.pipeline.lib.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.ChooserValues;

import java.util.ArrayList;
import java.util.List;

public class LocaleValuesProvider implements ChooserValues {
    @Override
    public List<String> getValues() {
      List<String> values = new ArrayList<>();
      for (FieldTypeConverterProcessor.DataLocale locale : FieldTypeConverterProcessor.DataLocale.values()) {
        values.add(locale.toString());
      }
      return values;
    }

    @Override
    public List<String> getLabels() {
      List<String> labels = new ArrayList<>();
      for (FieldTypeConverterProcessor.DataLocale locale : FieldTypeConverterProcessor.DataLocale.values()) {
        labels.add(locale.name());
      }
      return labels;
    }
  }