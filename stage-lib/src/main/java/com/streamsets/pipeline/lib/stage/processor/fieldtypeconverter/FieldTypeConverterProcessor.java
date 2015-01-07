/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldtypeconverter;

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
import java.util.List;
import java.util.Locale;

@GenerateResourceBundle
@StageDef(version="1.0.0", label="Field Type Converter")
public class FieldTypeConverterProcessor extends SingleLaneRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FieldTypeConverterProcessor.class);

  @ConfigDef(label = "Field Type Converter Configuration", required = false, type = Type.MODEL, defaultValue="",
    description="Fields whose type must be converted from String type to the target type.")
  @ComplexField
  public List<FieldTypeConverterConfig> fieldTypeConverterConfigs;

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {

    for(FieldTypeConverterConfig fieldTypeConverterConfig : fieldTypeConverterConfigs) {
      for(String fieldToConvert : fieldTypeConverterConfig.fields) {
        Field field = record.get(fieldToConvert);
        if(field == null) {
          LOG.warn("Record {} does not have field {}. Ignoring conversion.", record.getHeader().getSourceId(),
            fieldToConvert);
        } else if(field.getType() != Field.Type.STRING) {
          LOG.warn("Field {} in record {} is not of type String. Ignoring conversion.", fieldToConvert,
            record.getHeader().getSourceId());
        } else if(field.getValue() == null) {
          LOG.warn("Field {} in record {} has null value. Ignoring conversion.", fieldToConvert,
            record.getHeader().getSourceId());
        } else {
          try {
            record.set(fieldToConvert, convertToType(field, fieldTypeConverterConfig.targetType,
              fieldTypeConverterConfig.dataLocale.getLocale(), fieldTypeConverterConfig.dateFormat));
          } catch (ParseException | NumberFormatException e) {
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
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue).byteValue());
      case BYTE_ARRAY:
        return Field.create(stringValue.getBytes());
      case CHAR:
        return Field.create(stringValue.charAt(0));
      case DATE:
      case DATETIME:
        java.text.DateFormat dateFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
        return Field.createDate(dateFormat.parse(stringValue));
      case DECIMAL:
        Number decimal = NumberFormat.getInstance(dataLocale).parse(stringValue);
        return Field.create(new BigDecimal(decimal.toString()));
      case DOUBLE:
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue).doubleValue());
      case FLOAT:
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue).floatValue());
      case INTEGER:
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue).intValue());
      case LONG:
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue).longValue());
      case SHORT:
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue).shortValue());
      default:
        //return String field

        return field;
    }
  }

  public static class FieldTypeConverterConfig {

    @ConfigDef(label = "Fields to convert", required = true,type = Type.MODEL, defaultValue="",
      description="The fields whose type must be converted from String to the specified type.")
    @FieldSelector
    public List<String> fields;

    @ConfigDef(label = "Target type", required = true, type = Type.MODEL, defaultValue="INTEGER",
      description="The new type to which the string field must be converted to.")
    @ValueChooser(chooserValues = ConverterValuesProvider.class, type = ChooserMode.PROVIDED)
    public FieldType targetType;

    @ConfigDef(label = "Data Locale", required = true, type = Type.MODEL, defaultValue="ENGLISH",
      description="The current locale of the data which must be converted. " +
        "This is required to convert string with ',' and '.' to number types.")
    @ValueChooser(chooserValues = LocaleValuesProvider.class, type = ChooserMode.PROVIDED)
    public DataLocale dataLocale;

    @ConfigDef(label = "Date Format", required = true, type = Type.MODEL, defaultValue="yyyy-MM-dd",
      description="The format of the date into which the string field must be converted to. " +
        "This option is used only if the target type is Date or Date time.")
    @ValueChooser(chooserValues = DateFormatValuesProvider.class, type = ChooserMode.SUGGESTED)
    public String dateFormat;

  }

  public enum FieldType {
    BOOLEAN,
    CHAR,
    BYTE,
    SHORT,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    DATE,
    DATETIME,
    DECIMAL,
    STRING,
    BYTE_ARRAY
    //MAP and LIST are not supported
  }

  public enum DataLocale {

    ENGLISH(Locale.ENGLISH),
    FRENCH(Locale.FRENCH),
    GERMAN(Locale.GERMAN),
    ITALIAN(Locale.ITALIAN),
    JAPANESE(Locale.JAPANESE),
    KOREAN(Locale.KOREAN),
    CHINESE(Locale.CHINESE),
    SIMPLIFIED_CHINESE(Locale.SIMPLIFIED_CHINESE),
    TRADITIONAL_CHINESE(Locale.TRADITIONAL_CHINESE);

    private Locale locale;

    private DataLocale(Locale locale) {
      this.locale = locale;
    }

    public Locale getLocale() {
      return this.locale;
    }
  }

  public enum StandardDateFormats {

    YYYY_MM_DD("yyyy-MM-dd"),
    DD_MM_YYYY("dd-MMM-YYYY"),
    YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"),
    YYYY_MM_DD_HH_MM_SS_SSS("yyyy-MM-dd HH:mm:ss.SSS"),
    YYYY_MM_DD_HH_MM_SS_SSS_Z("yyyy-MM-dd HH:mm:ss.SSS Z");

    private String format;

    private StandardDateFormats(String format) {
      this.format = format;
    }

    public String getFormat() {
      return format;
    }
  }
}
