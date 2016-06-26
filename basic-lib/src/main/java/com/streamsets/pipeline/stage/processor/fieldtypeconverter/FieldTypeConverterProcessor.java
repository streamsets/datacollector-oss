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
import com.streamsets.pipeline.config.DateFormat;
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
import java.util.Set;

public class FieldTypeConverterProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(FieldTypeConverterProcessor.class);

  private final List<FieldTypeConverterConfig> fieldTypeConverterConfigs;

  public FieldTypeConverterProcessor(
      List<FieldTypeConverterConfig> fieldTypeConverterConfigs) {
    this.fieldTypeConverterConfigs = fieldTypeConverterConfigs;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> fieldPaths = record.getEscapedFieldPaths();
    for(FieldTypeConverterConfig fieldTypeConverterConfig : fieldTypeConverterConfigs) {
      for(String fieldToConvert : fieldTypeConverterConfig.fields) {
        for(String matchingField : FieldRegexUtil.getMatchingFieldPaths(fieldToConvert, fieldPaths)) {
          Field field = record.get(matchingField);
          if (field == null) {
            LOG.warn("Record {} does not have field {}. Ignoring conversion.", record.getHeader().getSourceId(),
              matchingField);
          } else {
            if (field.getType() == Field.Type.STRING) {
              if (field.getValue() == null) {
                LOG.warn("Field {} in record {} has null value. Converting the type of field to '{}' with null value.",
                  matchingField, record.getHeader().getSourceId(), fieldTypeConverterConfig.targetType);
                record.set(matchingField, Field.create(fieldTypeConverterConfig.targetType, null));
              } else {
                try {
                  String dateMask = null;
                  if (fieldTypeConverterConfig.targetType == Field.Type.DATE ||
                    fieldTypeConverterConfig.targetType == Field.Type.DATETIME ||
                    fieldTypeConverterConfig.targetType == Field.Type.TIME) {
                    dateMask = (fieldTypeConverterConfig.dateFormat != DateFormat.OTHER)
                      ? fieldTypeConverterConfig.dateFormat.getFormat()
                      : fieldTypeConverterConfig.otherDateFormat;
                  }
                  record.set(matchingField, convertStringToTargetType(field, fieldTypeConverterConfig.targetType,
                    fieldTypeConverterConfig.getLocale(), dateMask));
                } catch (ParseException | NumberFormatException e) {
                  throw new OnRecordErrorException(Errors.CONVERTER_00, matchingField, field.getValueAsString(),
                    fieldTypeConverterConfig.targetType.name(), e.toString(), e);

                }
              }
            } else if ((field.getType() == Field.Type.DATETIME || field.getType() == Field.Type.DATE || field.getType() == Field.Type.TIME) &&
                (fieldTypeConverterConfig.targetType == Field.Type.LONG ||
                    fieldTypeConverterConfig.targetType == Field.Type.STRING)) {
              if (field.getValue() == null) {
                LOG.warn("Field {} in record {} has null value. Converting the type of field to '{}' with null value.",
                    matchingField, record.getHeader().getSourceId(), fieldTypeConverterConfig.targetType);
                record.set(matchingField, Field.create(fieldTypeConverterConfig.targetType, null));
              } else if(fieldTypeConverterConfig.targetType == Field.Type.LONG) {
                record.set(matchingField, Field.create(fieldTypeConverterConfig.targetType,
                    field.getValueAsDatetime().getTime()));
              } else if(fieldTypeConverterConfig.targetType == Field.Type.STRING) {
                String dateMask = (fieldTypeConverterConfig.dateFormat != DateFormat.OTHER)
                    ? fieldTypeConverterConfig.dateFormat.getFormat()
                    : fieldTypeConverterConfig.otherDateFormat;
                java.text.DateFormat dateFormat = new SimpleDateFormat(dateMask, Locale.ENGLISH);
                record.set(matchingField, Field.create(fieldTypeConverterConfig.targetType,
                    dateFormat.format(field.getValueAsDatetime())));
              }
            } else {
              //use the built in type conversion provided by TypeSupport
              try {
                //use the built in type conversion provided by TypeSupport
                record.set(matchingField, Field.create(fieldTypeConverterConfig.targetType, field.getValue()));
              } catch (IllegalArgumentException e) {
                throw new OnRecordErrorException(Errors.CONVERTER_00, matchingField, field.getValueAsString(),
                    fieldTypeConverterConfig.targetType.name(), e.toString(), e);
              }
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
      default:
        return field;
    }
  }

}
