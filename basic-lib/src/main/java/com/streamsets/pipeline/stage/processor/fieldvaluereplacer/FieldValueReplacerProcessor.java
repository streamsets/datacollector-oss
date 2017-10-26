/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.lib.el.FieldEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.util.FieldPathExpressionUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class FieldValueReplacerProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(FieldValueReplacerProcessor.class);
  private final List<NullReplacerConditionalConfig> nullReplacerConditionalConfigs;
  private final List<FieldValueReplacerConfig> fieldsToReplaceIfNull;
  private final OnStagePreConditionFailure onStagePreConditionFailure;
  private final List<FieldValueConditionalReplacerConfig> fieldsToConditionallyReplace;
  private ELEval nullConditionELEval;
  private ELVars nullConditionELVars;
  private ELEval fieldPathEval;
  private ELVars fieldPathVars;

  public FieldValueReplacerProcessor(
      List<NullReplacerConditionalConfig> nullReplacerConditionalConfigs,
      List<FieldValueReplacerConfig> fieldsToReplaceIfNull,
      List<FieldValueConditionalReplacerConfig> fieldsToConditionallyReplace,
      OnStagePreConditionFailure onStagePreConditionFailure) {
    this.nullReplacerConditionalConfigs = nullReplacerConditionalConfigs;
    this.fieldsToReplaceIfNull = fieldsToReplaceIfNull;
    this.fieldsToConditionallyReplace = fieldsToConditionallyReplace;
    this.onStagePreConditionFailure = onStagePreConditionFailure;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> configIssues = super.init();
    if (configIssues.isEmpty()) {
      nullConditionELEval = getContext().createELEval("condition");
      nullConditionELVars = getContext().createELVars();
    }

    fieldPathEval = getContext().createELEval("fields");
    fieldPathVars = getContext().createELVars();

    return configIssues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> fieldPaths = record.getEscapedFieldPaths();
    Set<String> fieldsThatDoNotExist = new HashSet<>();

    ELEval fieldPathEval = getContext().createELEval(String.format(
        "%s---%s---%s",
        getContext().getPipelineId(),
        getContext().getStageInfo().getInstanceName(),
        "fieldPathEval"
    ), RecordEL.class, FieldEL.class);
    ELVars fieldPathVars = getContext().createELVars();

    RecordEL.setRecordInContext(nullConditionELVars, record);

    if(nullReplacerConditionalConfigs != null && !nullReplacerConditionalConfigs.isEmpty()) {
      for (String fieldToNull : getFieldsToNull(nullReplacerConditionalConfigs, fieldsThatDoNotExist, fieldPaths, record)) {
        Field field = record.get(fieldToNull);
        record.set(fieldToNull, Field.create(field.getType(), null));
      }
    }

    if(fieldsToReplaceIfNull !=null && !fieldsToReplaceIfNull.isEmpty()) {
      for (FieldValueReplacerConfig fieldValueReplacerConfig : fieldsToReplaceIfNull) {
        for (String fieldToReplace : fieldValueReplacerConfig.fields) {
          final List<String> matchingPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(fieldToReplace,
              fieldPathEval,
              fieldPathVars,
              record
          );
          if (matchingPaths.isEmpty()) {
            fieldsThatDoNotExist.add(fieldToReplace);
          } else {
            for(String matchingField : matchingPaths) {
              if (record.has(matchingField)) {
                Field field = record.get(matchingField);
                if (field.getValue() == null) {
                  try {
                    record.set(matchingField, Field.create(field, convertToType(
                        fieldValueReplacerConfig.newValue, field.getType(), "Replace If Null"))
                    );
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
    }

    if (fieldsToConditionallyReplace != null && !fieldsToConditionallyReplace.isEmpty()) {
      for (FieldValueConditionalReplacerConfig fieldValueConditionalReplacerConfig : fieldsToConditionallyReplace) {
        String operator = fieldValueConditionalReplacerConfig.operator;

        for (String fieldToReplace : fieldValueConditionalReplacerConfig.fieldNames) {
          for (String matchingField : FieldPathExpressionUtil.evaluateMatchingFieldPaths(
              fieldToReplace,
              fieldPathEval,
              fieldPathVars,
              record
          )) {
            if (record.has(matchingField)) {

              Field field = record.get(matchingField);
              Field.Type type = field.getType();

              // always need a valid "replacementValue"
              if ((fieldValueConditionalReplacerConfig.replacementValue == null) ||
                  fieldValueConditionalReplacerConfig.replacementValue
                      .trim()
                      .equals("") || fieldValueConditionalReplacerConfig.replacementValue.isEmpty()) {
                throw new IllegalArgumentException(Utils.format(Errors.VALUE_REPLACER_05.getMessage()));
              }

              try {
                // won't use "comparisonValue" when processing "ALL"-style replacement.
                if (operator.equals(OperatorChooserValues.ALL.name())) {
                  record.set(matchingField,
                      Field.create(field, convertToType(fieldValueConditionalReplacerConfig.replacementValue, type, matchingField))
                  );
                  continue;
                }

                // from here on we need a valid "comparisonValue" value for comparison.
                if ((
                    (fieldValueConditionalReplacerConfig.comparisonValue == null) || fieldValueConditionalReplacerConfig.comparisonValue
                        .trim()
                        .equals("") || fieldValueConditionalReplacerConfig.comparisonValue.isEmpty()
                )) {
                  throw new IllegalArgumentException(Utils.format(Errors.VALUE_REPLACER_04.getMessage()));
                }

                int val = compareIt(field, fieldValueConditionalReplacerConfig.comparisonValue, matchingField);
                if (val == 0 && operator.equals(OperatorChooserValues.EQUALS.name())) {
                  record.set(
                      matchingField,
                      Field.create(field, convertToType(fieldValueConditionalReplacerConfig.replacementValue, type, matchingField))
                  );

                } else if (val < 0 && operator.equals(OperatorChooserValues.LESS_THAN.name())) {
                  record.set(
                      matchingField,
                      Field.create(field, convertToType(fieldValueConditionalReplacerConfig.replacementValue, type, matchingField))
                  );

                } else if (val > 0 && operator.equals(OperatorChooserValues.GREATER_THAN.name())) {
                  record.set(
                      matchingField,
                      Field.create(field, convertToType(fieldValueConditionalReplacerConfig.replacementValue, type, matchingField))
                  );
                }

              } catch (Exception e) {
                throw new IllegalArgumentException(Utils.format(
                    Errors.VALUE_REPLACER_03.getMessage(),
                    field.getType(), matchingField
                ));
              }
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

  //This function simply evaluates the condition in each nullReplacerConditionalConfig and gather all fields that
  //should be replaced by null.
  private List<String> getFieldsToNull(List<NullReplacerConditionalConfig> nullReplacerConditionalConfigs, Set<String> fieldsThatDoNotExist, Set<String> fieldPaths, Record record) throws OnRecordErrorException {
    //Gather in this all fields to null
    List<String> fieldsToNull = new ArrayList<>();

    for (NullReplacerConditionalConfig nullReplacerConditionalConfig : nullReplacerConditionalConfigs) {
      List<String> fieldNamesToNull = nullReplacerConditionalConfig.fieldsToNull;
      //Gather fieldsPathsToNull for this nullReplacerConditionalConfig
      List<String> fieldPathsToNull = new ArrayList<>();
      //Gather existing paths for each nullReplacerConditionalConfig
      //And if field does not exist gather them in fieldsThatDoNotExist
      for (String fieldNameToNull : fieldNamesToNull) {
        try {
          final List<String> matchingPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(
              fieldNameToNull, fieldPathEval, fieldPathVars,
              record
          );
          if (matchingPaths.isEmpty()) {
            // FieldPathExpressionUtil.evaluateMatchingFieldPaths does NOT return the supplied param in its result
            // regardless, like FieldRegexUtil#getMatchingFieldPaths did, so we add manually here
            fieldsThatDoNotExist.add(fieldNameToNull);
          } else {
            for (String matchingField : matchingPaths) {
              if (record.has(matchingField)) {
                fieldPathsToNull.add(matchingField);
              } else {
                fieldsThatDoNotExist.add(matchingField);
              }
            }
          }
        } catch (ELEvalException e) {
          LOG.error("Error evaluating condition: " + nullReplacerConditionalConfig.condition, e);
          throw new OnRecordErrorException(record, Errors.VALUE_REPLACER_07, fieldNameToNull, e.toString(), e);
        }
      }
      //Now evaluate the condition in nullReplacerConditionalConfig
      //If it empty or condition evaluates to true, add all the gathered fields in fieldsPathsToNull
      // for this nullReplacerConditionalConfig to fieldsToNull
      try {
        boolean evaluatedCondition = true;
        //If it is empty we assume it is true.
        if (!StringUtils.isEmpty(nullReplacerConditionalConfig.condition)) {
          evaluatedCondition = nullConditionELEval.eval(nullConditionELVars, nullReplacerConditionalConfig.condition, Boolean.class);
        }
        if (evaluatedCondition) {
          fieldsToNull.addAll(fieldPathsToNull);
        }
      } catch (ELEvalException e) {
        LOG.error("Error evaluating condition: " + nullReplacerConditionalConfig.condition, e);
        throw new OnRecordErrorException(record, Errors.VALUE_REPLACER_06, nullReplacerConditionalConfig.condition, e.toString());
      }
    }
    return fieldsToNull;
  }

  private int compareIt(Field field, String stringValue, String matchingField) {
    try {
      switch (field.getType()) {
        case BYTE:
          if (field.getValueAsByte() == (Byte) convertToType(stringValue, field.getType(), matchingField)) {
            return 0;
          } else if (field.getValueAsByte() < (Byte) convertToType(stringValue, field.getType(), matchingField)) {
            return -1;
          } else {
            return 1;
          }

        case SHORT:
          if (field.getValueAsShort() == (Short) convertToType(stringValue, field.getType(), matchingField)) {
            return 0;
          } else if (field.getValueAsShort() < (Short) convertToType(stringValue, field.getType(), matchingField)) {
            return -1;
          } else {
            return 1;
          }

        case INTEGER:
          if (field.getValueAsInteger() == (Integer) convertToType(stringValue, field.getType(), matchingField)) {
            return 0;
          } else if (field.getValueAsInteger() < (Integer) convertToType(stringValue, field.getType(), matchingField)) {
            return -1;
          } else {
            return 1;
          }

        case LONG:
          if (field.getValueAsLong() == (Long) convertToType(stringValue, field.getType(), matchingField)) {
            return 0;
          } else if (field.getValueAsLong() < (Long) convertToType(stringValue, field.getType(), matchingField)) {
            return -1;
          } else {
            return 1;
          }

        case FLOAT:
          if (field.getValueAsFloat() == (Float) convertToType(stringValue, field.getType(), matchingField)) {
            return 0;
          } else if (field.getValueAsFloat() < (Float) convertToType(stringValue, field.getType(), matchingField)) {
            return -1;
          } else {
            return 1;
          }

        case DOUBLE:
          if (field.getValueAsDouble() == (Double) convertToType(stringValue, field.getType(), matchingField)) {
            return 0;
          } else if (field.getValueAsDouble() < (Double) convertToType(stringValue, field.getType(), matchingField)) {
            return -1;
          } else {
            return 1;
          }

        case STRING:
          return field.getValueAsString().compareTo(stringValue);

        default:
          throw new IllegalArgumentException(Utils.format(Errors.VALUE_REPLACER_03.getMessage(), field.getType(), matchingField));
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(Utils.format(Errors.VALUE_REPLACER_03.getMessage(), field.getType()));
    }
  }

  private Object convertToType(String stringValue, Field.Type fieldType, String matchingField) throws ParseException {
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
      case TIME:
        DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss", Locale.ENGLISH);
        return timeFormat.parse(stringValue);
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
      case FILE_REF:
        throw new IllegalArgumentException(Utils.format(Errors.VALUE_REPLACER_03.getMessage(), fieldType, matchingField));
      case LIST_MAP:
      case LIST:
      case MAP:
      default:
        return stringValue;
    }
  }
}
