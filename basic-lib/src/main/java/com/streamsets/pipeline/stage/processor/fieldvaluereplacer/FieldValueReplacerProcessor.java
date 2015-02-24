/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldvaluereplacer;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

@GenerateResourceBundle
@StageDef(
    version="1.0.0",
    label="Value Replacer",
    description = "Replaces null values with a constant and replaces values with NULL",
    icon="replacer.svg"
)
@ConfigGroups(Groups.class)
public class FieldValueReplacerProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(FieldValueReplacerProcessor.class);

  @ConfigDef(
      required = false,
      type = Type.MODEL,
      defaultValue="",
      label = "Fields to Null",
      description="Replaces existing values with null values",
      displayPosition = 10,
      group = "REPLACE"
  )
  @FieldSelector
  public List<String> fieldsToNull;

  @ConfigDef(
      required = false,
      type = Type.MODEL, defaultValue="",
      label = "Replace Null Values",
      description="Replaces the null values in a field with a specified value.",
      displayPosition = 20,
      group = "REPLACE"
  )
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
              try {
                record.set(fieldToReplace, Field.create(field, convertToType(
                  fieldValueReplacerConfig.newValue, field.getType())));
              } catch (Exception e) {
                throw new OnRecordErrorException(Errors.VALUE_REPLACER_00, fieldValueReplacerConfig.newValue,
                  field.getType(), e.getMessage(), e);
              }
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

  private Object convertToType(String stringValue, Field.Type fieldType) throws ParseException {
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
        return dateFormat.parse(stringValue);
      case DATETIME:
        DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ", Locale.ENGLISH);
        return dateTimeFormat.parse(stringValue);
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

}