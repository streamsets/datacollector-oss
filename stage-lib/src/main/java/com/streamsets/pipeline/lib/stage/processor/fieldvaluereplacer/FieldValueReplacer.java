/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldvaluereplacer;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

@GenerateResourceBundle
@StageDef( version="1.0.0", label="Field Value Replacer")
public class FieldValueReplacer extends SingleLaneRecordProcessor {

  @ConfigDef(label = "Fields to replace with null", required = false, type = Type.MODEL, defaultValue="",
    description="The fields whose values must be replaced with nulls")
  @FieldSelector
  public List<String> fieldsToNull;

  @ConfigDef(label = "Fields with null values to be replaced", required = false, type = Type.MODEL, defaultValue="",
    description="Fields whose values, if null, to be replaced with the specified value")
  @ComplexField
  public List<FieldValueReplacerConfig> fieldsToReplaceIfNull;

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Record recordClone = getContext().cloneRecord(record);

    if(fieldsToNull != null && !fieldsToNull.isEmpty()) {
      for (String fieldToNull : fieldsToNull) {
        Field field = recordClone.get(fieldToNull);
        field.set(field.getType(), null);
      }
    }

    if(fieldsToReplaceIfNull !=null && !fieldsToReplaceIfNull.isEmpty()) {
      for (FieldValueReplacerConfig fieldValueReplacerConfig : fieldsToReplaceIfNull) {
        for (String fieldToReplace : fieldValueReplacerConfig.fields) {
          Field field = recordClone.get(fieldToReplace);
          if (field.getValue() == null) {
            field.set(field.getType(), convertToType(fieldValueReplacerConfig.newValue, field.getType()));
          }
        }
      }
    }

    batchMaker.addRecord(recordClone);
  }

  private Object convertToType(String stringValue, Field.Type fieldType) {
    switch (fieldType) {
      case BOOLEAN:
        return Boolean.valueOf(stringValue);
      case BYTE:
        return Byte.valueOf(stringValue);
      case BYTE_ARRAY:
        return stringValue.getBytes();
      case CHAR:
        return stringValue.charAt(0);
      case DATE:
        DateFormat dateFormat = new SimpleDateFormat("MMMM d, yyyy", Locale.ENGLISH);
        Date date;
        try {
          date = dateFormat.parse(stringValue);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        return date;
      case DATETIME:
        DateFormat dateTimeFormat = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z", Locale.ENGLISH);
        Date dateTime;
        try {
          dateTime = dateTimeFormat.parse(stringValue);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        return dateTime;
      case DECIMAL:
        return new BigDecimal(stringValue);
      case DOUBLE:
        return Double.valueOf(stringValue);
      case FLOAT:
        return Float.valueOf(stringValue);
      case INTEGER:
        return Integer.valueOf(stringValue);
      case LONG:
        return Long.valueOf(stringValue);
      case LIST:
      case MAP:
      case SHORT:
        return Short.valueOf(stringValue);
      default:
        return stringValue;
    }
  }

  public static class FieldValueReplacerConfig {

    @ConfigDef(label = "Fields to replace", required = true,type = Type.MODEL, defaultValue="",
      description="The fields which must be replaced with the given value if the current value is null")
    @FieldSelector
    public List<String> fields;

    @ConfigDef(label = "New value", required = true,type = Type.STRING, defaultValue="",
      description="The new value which must be set if the current value is null")
    public String newValue;

  }
}