package com.streamsets.pipeline.lib.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.Field;

import java.util.ArrayList;
import java.util.List;

public class ConverterValuesProvider implements ChooserValues {
    @Override
    public List<String> getValues() {
      List<String> values = new ArrayList<>();
      for (Field.Type type : Field.Type.values()) {
        if(type != Field.Type.MAP && type != Field.Type.LIST) {
          values.add(type.toString());
        }
      }
      return values;
    }

    @Override
    public List<String> getLabels() {
      List<String> labels = new ArrayList<>();
      for (Field.Type type : Field.Type.values()) {
        if(type != Field.Type.MAP && type != Field.Type.LIST) {
          labels.add(type.toString());
        }
      }
      return labels;
    }
  }