/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.FieldRegexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class FieldTypeConverterProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(FieldTypeConverterProcessor.class);

  private final ConvertBy convertBy;
  private final List<FieldTypeConverterConfig> fieldTypeConverterConfigs;
  private final List<WholeTypeConverterConfig> wholeTypeConverterConfigs;

  public FieldTypeConverterProcessor(
      ConvertBy convertBy,
      List<FieldTypeConverterConfig> fieldTypeConverterConfigs,
      List<WholeTypeConverterConfig> wholeTypeConverterConfigs
  ) {
    this.convertBy = convertBy;
    this.fieldTypeConverterConfigs = fieldTypeConverterConfigs;
    this.wholeTypeConverterConfigs = wholeTypeConverterConfigs;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
      switch (convertBy) {
        case BY_TYPE:
          Field rootField = record.get();
          if (rootField != null) {
            record.set(processByType("", rootField));
          }
          batchMaker.addRecord(record);
          break;
        case BY_FIELD:
          processByField(record, batchMaker);
          break;
        default:
          throw new IllegalArgumentException("Unknown convert by type: " + convertBy);
      }
  }

  private Field processByType(String matchingPath, Field rootField) throws StageException {
    switch (rootField.getType()) {
      case MAP:
      case LIST_MAP:
        for (Map.Entry<String, Field> entry : rootField.getValueAsMap().entrySet()) {
          entry.setValue(processByType(matchingPath + "/" + entry.getKey(), entry.getValue()));
        }
        break;
      case LIST:
        List<Field> fields = rootField.getValueAsList();
        for(int i = 0; i < fields.size(); i++) {
          fields.set(i, processByType(matchingPath + "[" + i + "]", fields.get(i)));
        }
        break;
      default:
        for(WholeTypeConverterConfig converterConfig : wholeTypeConverterConfigs) {
          if(converterConfig.sourceType == rootField.getType()) {
            rootField = convertField(matchingPath, rootField, converterConfig);
          }
        }
    }

    // Return original field
    return rootField;
  }

  private void processByField(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> fieldPaths = record.getEscapedFieldPaths();
    for(FieldTypeConverterConfig fieldTypeConverterConfig : fieldTypeConverterConfigs) {
      for(String fieldToConvert : fieldTypeConverterConfig.fields) {
        for(String matchingField : FieldRegexUtil.getMatchingFieldPaths(fieldToConvert, fieldPaths)) {
          Field field = record.get(matchingField);
          if(field == null) {
            LOG.warn("Record does not have field {}. Ignoring conversion.", matchingField);
          } else {
            record.set(matchingField, convertField(matchingField, field, fieldTypeConverterConfig));
          }
        }
      }
    }
    batchMaker.addRecord(record);
  }

  private Field convertField(String matchingField, Field field, BaseConverterConfig converterConfig) throws StageException {
    if (field.getType() == Field.Type.STRING) {
      if (field.getValue() == null) {
        LOG.warn("Field {} has null value. Converting the type of field to '{}' with null value.", matchingField, converterConfig.targetType);
        return Field.create(converterConfig.targetType, null);
      } else {
        try {
          String dateMask = null;
          if (converterConfig.targetType.isOneOf(Field.Type.DATE, Field.Type.DATETIME, Field.Type.TIME)) {
            dateMask = converterConfig.getDateMask();
          }
          return convertStringToTargetType(field, converterConfig.targetType, converterConfig.getLocale(), dateMask);
        } catch (ParseException | IllegalArgumentException e) {
          throw new OnRecordErrorException(Errors.CONVERTER_02,
              matchingField,
              field.getValueAsString(),
              converterConfig.targetType.name()
          );
        }
      }
    }

    if (field.getType().isOneOf(Field.Type.DATETIME, Field.Type.DATE, Field.Type.TIME) && converterConfig.targetType.isOneOf(Field.Type.LONG, Field.Type.STRING)) {
      if (field.getValue() == null) {
        LOG.warn("Field {} in record has null value. Converting the type of field to '{}' with null value.", matchingField, converterConfig.targetType);
        return Field.create(converterConfig.targetType, null);
      } else if(converterConfig.targetType == Field.Type.LONG) {
        return Field.create(converterConfig.targetType, field.getValueAsDatetime().getTime());
      } else if(converterConfig.targetType == Field.Type.STRING) {
        String dateMask = converterConfig.getDateMask();
        java.text.DateFormat dateFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
        return Field.create(converterConfig.targetType, dateFormat.format(field.getValueAsDatetime()));
      }
    }

    if(field.getType() == Field.Type.BYTE_ARRAY && converterConfig.targetType == Field.Type.STRING) {
      try {
        return Field.create(converterConfig.targetType, new String(field.getValueAsByteArray(), converterConfig.encoding));
      } catch (Exception e) {
        throw new OnRecordErrorException(Errors.CONVERTER_01, converterConfig.encoding);
      }
    }

    // Use the built in type conversion provided by TypeSupport
    try {
      // Use the built in type conversion provided by TypeSupport
      return Field.create(converterConfig.targetType, field.getValue());
    } catch (IllegalArgumentException e) {
      throw new OnRecordErrorException(Errors.CONVERTER_02,
          matchingField,
          field.getValueAsString(),
          converterConfig.targetType.name()
      );
    }
  }

  private Field convertStringToTargetType(Field field, Field.Type targetType, Locale dataLocale, String dateMask)
    throws ParseException {
    String stringValue = field.getValueAsString();
    switch(targetType) {
      case BOOLEAN:
        return Field.create(Boolean.valueOf(stringValue));
      case BYTE:
        return Field.create(NumberFormat.getInstance(dataLocale).parse(stringValue).byteValue());
      case BYTE_ARRAY:
        return Field.create(stringValue.getBytes(StandardCharsets.UTF_8));
      case CHAR:
        return Field.create(stringValue.charAt(0));
      case DATE:
        java.text.DateFormat dateFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
        return Field.createDate(dateFormat.parse(stringValue));
      case DATETIME:
        java.text.DateFormat dateTimeFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
        return Field.createDatetime(dateTimeFormat.parse(stringValue));
      case TIME:
        java.text.DateFormat timeFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
        return Field.createTime(timeFormat.parse(stringValue));
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
      case FILE_REF:
        throw new IllegalArgumentException(Utils.format("Cannot convert String value to type {}", targetType));
      default:
        return field;
    }
  }

}
