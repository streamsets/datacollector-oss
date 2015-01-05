/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.devtest.fieldtypeconverter;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

@GenerateResourceBundle
@StageDef(version="1.0.0", label="Dummy Field Type Converter")
public class FieldTypeConverterProcessor extends SingleLaneRecordProcessor {

  @ConfigDef(label = "Fields to be converted", required = false, type = Type.MODEL, defaultValue="",
    description="Fields whose values, if null, to be replaced with the specified value")
  @ComplexField
  public List<FieldTypeConverterConfig> fieldTypeConverterConfigs;

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {

    for(FieldTypeConverterConfig fieldTypeConverterConfig : fieldTypeConverterConfigs) {
      for(String fieldToConvert : fieldTypeConverterConfig.fields) {
        Field field = record.get(fieldToConvert);
        try {
          record.set(fieldToConvert, convertToType(field.getValueAsString(), fieldTypeConverterConfig.targetType,
            fieldTypeConverterConfig.dataLocale.getLocale(), fieldTypeConverterConfig.dateMask.getFormat()));
        } catch (ParseException e) {
          //FIXME<Hari>: add message
          getContext().toError(record, e);
        }
      }
    }

    batchMaker.addRecord(record);
  }

  public Field convertToType(String stringValue, FieldType fieldType, Locale dataLocale, String dateMask) throws ParseException {
    switch(fieldType) {
      case BOOLEAN:
        return Field.create(Boolean.valueOf(stringValue));
      case BYTE:
        return Field.create(Byte.valueOf(stringValue));
      case BYTE_ARRAY:
        return Field.create(stringValue.getBytes());
      case CHAR:
        return Field.create(stringValue.charAt(0));
      case DATE:
      case DATETIME:
        DateFormat dateFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
        Date date;
        try {
          date = dateFormat.parse(stringValue);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        return Field.createDate(date);
      case DECIMAL:
        Number decimal = NumberFormat.getInstance(dataLocale).parse(stringValue);
        return Field.create(new BigDecimal(decimal.toString()));
      case DOUBLE:
        Number d = NumberFormat.getInstance(dataLocale).parse(stringValue);
        return Field.create(Double.valueOf(d.toString()));
      case FLOAT:
        Number f = NumberFormat.getInstance(dataLocale).parse(stringValue);
        return Field.create(Float.valueOf(f.toString()));
      case INTEGER:
        Number i = NumberFormat.getInstance(dataLocale).parse(stringValue);
        return Field.create(Integer.valueOf(i.toString()));
      case LONG:
        Number l = NumberFormat.getInstance(dataLocale).parse(stringValue);
        return Field.create(Long.valueOf(l.toString()));
      case SHORT:
        Number s = NumberFormat.getInstance(dataLocale).parse(stringValue);
        return Field.create(Short.valueOf(s.toString()));
      default:
        return Field.create(stringValue);
    }
  }

  public static class FieldTypeConverterConfig {

    @ConfigDef(label = "Fields to convert", required = true,type = Type.MODEL, defaultValue="",
      description="The fields whose type must be converted from String to the specified type.")
    @FieldSelector
    public List<String> fields;

    @ConfigDef(label = "Target type", required = true, type = Type.MODEL, defaultValue="",
      description="The new type to which the field must be converted to.")
    @ValueChooser(chooserValues = ConverterValuesProvider.class, type = ChooserMode.PROVIDED)
    public FieldType targetType;

    @ConfigDef(label = "Data Locale", required = true, type = Type.MODEL, defaultValue="",
      description="The current locale of the data which must be converted.")
    @ValueChooser(chooserValues = LocaleValuesProvider.class, type = ChooserMode.PROVIDED)
    public DataLocale dataLocale;

    @ConfigDef(label = "Data Locale", required = true, type = Type.MODEL, defaultValue="",
      description="The current locale of the data which must be converted.")
    @ValueChooser(chooserValues = DateMaskValuesProvider.class, type = ChooserMode.PROVIDED)
    public DateMask dateMask;

  }
}
