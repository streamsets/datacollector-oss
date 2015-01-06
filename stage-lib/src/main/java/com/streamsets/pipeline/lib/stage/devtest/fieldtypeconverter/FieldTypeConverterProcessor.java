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
import com.streamsets.pipeline.lib.util.StageLibError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

@GenerateResourceBundle
@StageDef(version="1.0.0", label="Dummy Field Type Converter")
public class FieldTypeConverterProcessor extends SingleLaneRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FieldTypeConverterProcessor.class);

  @ConfigDef(label = "Fields to be converted", required = false, type = Type.MODEL, defaultValue="",
    description="Fields whose values, if null, to be replaced with the specified value")
  @ComplexField
  public List<FieldTypeConverterConfig> fieldTypeConverterConfigs;

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {

    for(FieldTypeConverterConfig fieldTypeConverterConfig : fieldTypeConverterConfigs) {
      for(String fieldToConvert : fieldTypeConverterConfig.fields) {
        Field field = record.get(fieldToConvert);
        if(field.getValue() == null) {
          LOG.warn("Encountered field {} with null value in record {}. Ignoring conversion.", fieldToConvert,
            record.getHeader().getSourceId());
        } else {
          try {
            record.set(fieldToConvert, convertToType(field, fieldTypeConverterConfig.targetType,
              fieldTypeConverterConfig.dataLocale.getLocale(), fieldTypeConverterConfig.dateFormat.getFormat()));
          } catch (ParseException e) {
            LOG.warn(StageLibError.LIB_0400.getMessage(), field.getValueAsString(),
              fieldTypeConverterConfig.targetType.name(), e.getMessage());
            getContext().toError(record, StageLibError.LIB_0400, field.getValueAsString(),
              fieldTypeConverterConfig.targetType.name(), e.getMessage(), e);
          }
        }
      }
    }

    batchMaker.addRecord(record);
  }

  public Field convertToType(Field field, FieldType fieldType, Locale dataLocale, String dateMask)
    throws ParseException {
    String stringValue = field.getValueAsString();
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
        if(field.getType() == Field.Type.LONG) {
          return Field.createDate(new Date(field.getValueAsLong()));
        } else {
          java.text.DateFormat dateFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
          return Field.createDate(dateFormat.parse(stringValue));
        }
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

    @ConfigDef(label = "Target type", required = true, type = Type.MODEL, defaultValue="INTEGER",
      description="The new type to which the field must be converted to.")
    @ValueChooser(chooserValues = ConverterValuesProvider.class, type = ChooserMode.PROVIDED)
    public FieldType targetType;

    @ConfigDef(label = "Data Locale", required = true, type = Type.MODEL, defaultValue="ENGLISH",
      description="The current locale of the data which must be converted.")
    @ValueChooser(chooserValues = LocaleValuesProvider.class, type = ChooserMode.PROVIDED)
    public DataLocale dataLocale;

    @ConfigDef(label = "Date Format", required = true, type = Type.MODEL, defaultValue="YYYY_MM_DD",
      description="The current locale of the data which must be converted.")
    @ValueChooser(chooserValues = DateFormatValuesProvider.class, type = ChooserMode.PROVIDED)
    public DateFormat dateFormat;

  }
}
