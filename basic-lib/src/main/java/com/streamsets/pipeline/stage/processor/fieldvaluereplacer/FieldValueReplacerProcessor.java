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
package com.streamsets.pipeline.stage.processor.fieldvaluereplacer;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.lib.util.FieldRegexUtil;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class FieldValueReplacerProcessor extends SingleLaneRecordProcessor {
  private final List<String> fieldsToNull;
  private final List<FieldValueReplacerConfig> fieldsToReplaceIfNull;
  private final OnStagePreConditionFailure onStagePreConditionFailure;

  public FieldValueReplacerProcessor(List<String> fieldsToNull,
      List<FieldValueReplacerConfig> fieldsToReplaceIfNull,
      OnStagePreConditionFailure onStagePreConditionFailure) {
    this.fieldsToNull = fieldsToNull;
    this.fieldsToReplaceIfNull = fieldsToReplaceIfNull;
    this.onStagePreConditionFailure = onStagePreConditionFailure;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> fieldPaths = record.getEscapedFieldPaths();
    Set<String> fieldsThatDoNotExist = new HashSet<>();
    if(fieldsToNull != null && !fieldsToNull.isEmpty()) {
      for (String fieldToNull : fieldsToNull) {
        for(String matchingField : FieldRegexUtil.getMatchingFieldPaths(fieldToNull, fieldPaths)) {
          if (record.has(matchingField)) {
            Field field = record.get(matchingField);
            record.set(matchingField, Field.create(field, null));
          } else {
            fieldsThatDoNotExist.add(matchingField);
          }
        }
      }
    }

    if(fieldsToReplaceIfNull !=null && !fieldsToReplaceIfNull.isEmpty()) {
      for (FieldValueReplacerConfig fieldValueReplacerConfig : fieldsToReplaceIfNull) {
        for (String fieldToReplace : fieldValueReplacerConfig.fields) {
          for(String matchingField : FieldRegexUtil.getMatchingFieldPaths(fieldToReplace, fieldPaths)) {
            if (record.has(matchingField)) {
              Field field = record.get(matchingField);
              if (field.getValue() == null) {
                try {
                  record.set(matchingField, Field.create(field, convertToType(
                    fieldValueReplacerConfig.newValue, field.getType())));
                } catch (IllegalArgumentException | ParseException e) {
                  throw new OnRecordErrorException(Errors.VALUE_REPLACER_00, fieldValueReplacerConfig.newValue,
                    field.getType(), e.toString(), e);
                }
              }
            } else {
              fieldsThatDoNotExist.add(matchingField);
            }
          }
        }
      }
    }

    if(onStagePreConditionFailure == OnStagePreConditionFailure.TO_ERROR && !fieldsThatDoNotExist.isEmpty()) {
     throw new OnRecordErrorException(Errors.VALUE_REPLACER_01, record.getHeader().getSourceId(),
       Joiner.on(", ").join(fieldsThatDoNotExist));
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
        return stringValue.getBytes(StandardCharsets.UTF_8);
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
      case SHORT:
        return Short.valueOf(stringValue);
      case LIST_MAP:
      case LIST:
      case MAP:
      default:
        return stringValue;
    }
  }
}