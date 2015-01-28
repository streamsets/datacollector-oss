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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

@GenerateResourceBundle
@StageDef( version="1.0.0", label="Value Replacer", icon="replacer.svg")
public class FieldValueReplacer extends SingleLaneRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FieldValueReplacer.class);

  @ConfigDef(label = "Fields to NULL", required = false, type = Type.MODEL, defaultValue="",
    description="Replaces field values with null value.")
  @FieldSelector
  public List<String> fieldsToNull;

  @ConfigDef(label = "Replace Null values", required = false, type = Type.MODEL, defaultValue="",
    description="Replaces the null values in a field with a specified value.")
  @ComplexField
  public List<FieldValueReplacerConfig> fieldsToReplaceIfNull;

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    if(fieldsToNull != null && !fieldsToNull.isEmpty()) {
      for (String fieldToNull : fieldsToNull) {
        if(record.has(fieldToNull)) {
          Field field = record.get(fieldToNull);
          record.set(fieldToNull, Field.create(field, null));
        } else {
          LOG.warn("Record {} does not have field {}. Ignoring field replacement.", record.getHeader().getSourceId(),
            fieldToNull);
        }
      }
    }

    if(fieldsToReplaceIfNull !=null && !fieldsToReplaceIfNull.isEmpty()) {
      for (FieldValueReplacerConfig fieldValueReplacerConfig : fieldsToReplaceIfNull) {
        for (String fieldToReplace : fieldValueReplacerConfig.fields) {
          if(record.has(fieldToReplace)) {
            Field field = record.get(fieldToReplace);
            if (field.getValue() == null) {
              record.set(fieldToReplace, Field.create(field, convertToType(
                fieldValueReplacerConfig.newValue, field.getType())));
            } else {
              LOG.debug("Field {} in Record {} is not null. Ignoring field replacement.", fieldToReplace,
                record.getHeader().getSourceId());
            }
          } else {
            LOG.warn("Record {} does not have field {}. Ignoring field replacement.", record.getHeader().getSourceId(),
              fieldToReplace);
          }
        }
      }
    }

    batchMaker.addRecord(record);
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
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        Date date;
        try {
          date = dateFormat.parse(stringValue);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        return date;
      case DATETIME:
        DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ", Locale.ENGLISH);
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

    @ConfigDef(label = "Fields to Replace", required = true,type = Type.MODEL, defaultValue="")
    @FieldSelector
    public List<String> fields;

    @ConfigDef(label = "Replacement value", required = true,type = Type.STRING, defaultValue="",
      description="Value to replace null values")
    public String newValue;

  }
}