/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

public class FieldTypeConverterProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(FieldTypeConverterProcessor.class);

  private final List<FieldTypeConverterConfig> fieldTypeConverterConfigs;

  public FieldTypeConverterProcessor(
      List<FieldTypeConverterConfig> fieldTypeConverterConfigs) {
    this.fieldTypeConverterConfigs = fieldTypeConverterConfigs;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    for(FieldTypeConverterConfig fieldTypeConverterConfig : fieldTypeConverterConfigs) {
      for(String fieldToConvert : fieldTypeConverterConfig.fields) {
        Field field = record.get(fieldToConvert);
        if(field == null) {
          LOG.warn("Record {} does not have field {}. Ignoring conversion.", record.getHeader().getSourceId(),
            fieldToConvert);
        } else {
          if(field.getType() == Field.Type.STRING) {
            if(field.getValue() == null) {
              LOG.warn("Field {} in record {} has null value. Converting the type of filed to '{}' with null value.",
              fieldToConvert, record.getHeader().getSourceId(), fieldTypeConverterConfig.targetType);
              record.set(fieldToConvert, Field.create(fieldTypeConverterConfig.targetType, null));
            } else {
              try {
                record.set(fieldToConvert, convertStringToTargetType(field, fieldTypeConverterConfig.targetType,
                  fieldTypeConverterConfig.getLocale(), fieldTypeConverterConfig.dateFormat));
              } catch (ParseException | NumberFormatException e) {
                getContext().toError(record, Errors.CONVERTER_00, fieldToConvert, field.getValueAsString(),
                  fieldTypeConverterConfig.targetType.name(), e.getMessage(), e);
                return;
              }
            }
          } else {
            try {
              //use the built in type conversion provided by TypeSupport
              record.set(fieldToConvert, Field.create(fieldTypeConverterConfig.targetType, field.getValue()));
            } catch (IllegalArgumentException e) {
              getContext().toError(record, Errors.CONVERTER_00, fieldToConvert, field.getValueAsString(),
                fieldTypeConverterConfig.targetType.name(), e.getMessage(), e);
              return;
            }
          }
        }
      }
    }
    batchMaker.addRecord(record);
  }

  public Field convertStringToTargetType(Field field, Field.Type targetType, Locale dataLocale, String dateMask)
    throws ParseException {
    String stringValue = field.getValueAsString();
    switch(targetType) {
      case BOOLEAN:
        return Field.create(Boolean.valueOf(stringValue));
      case BYTE:
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue).byteValue());
      case BYTE_ARRAY:
        return Field.create(stringValue.getBytes());
      case CHAR:
        return Field.create(stringValue.charAt(0));
      case DATE:
        java.text.DateFormat dateFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
        return Field.createDate(dateFormat.parse(stringValue));
      case DATETIME:
        java.text.DateFormat dateTimeFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
        return Field.createDatetime(dateTimeFormat.parse(stringValue));
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
        return field;
    }
  }

}
