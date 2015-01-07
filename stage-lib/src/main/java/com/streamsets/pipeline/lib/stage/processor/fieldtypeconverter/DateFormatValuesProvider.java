package com.streamsets.pipeline.lib.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.ChooserValues;

import java.util.ArrayList;
import java.util.List;

public class DateFormatValuesProvider implements ChooserValues {
    @Override
    public List<String> getValues() {
      List<String> values = new ArrayList<>();
      for (FieldTypeConverterProcessor.StandardDateFormats standardDateFormats : FieldTypeConverterProcessor.StandardDateFormats.values()) {
        values.add(standardDateFormats.getFormat());
      }
      return values;
    }

    @Override
    public List<String> getLabels() {
      List<String> labels = new ArrayList<>();
      for (FieldTypeConverterProcessor.StandardDateFormats standardDateFormats : FieldTypeConverterProcessor.StandardDateFormats.values()) {
        labels.add(standardDateFormats.getFormat());
      }
      return labels;
    }
  }