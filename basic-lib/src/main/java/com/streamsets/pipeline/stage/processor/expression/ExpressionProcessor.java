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
package com.streamsets.pipeline.stage.processor.expression;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringELConstants;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.util.FieldRegexUtil;
import com.streamsets.pipeline.lib.util.FieldUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExpressionProcessor extends SingleLaneRecordProcessor {

  private final List<ExpressionProcessorConfig> expressionProcessorConfigs;
  private final List<HeaderAttributeConfig> headerAttributeConfigs;
  private final List<FieldAttributeConfig> fieldAttributeConfigs;
  private final Map<String, ?> memoizedVars = new HashMap<>();

  private ELEval expressionEval;
  private ELVars expressionVars;
  private ELEval headerAttributeEval;
  private ELEval fieldAttributeEval;

  public ExpressionProcessor(
      List<ExpressionProcessorConfig> expressionProcessorConfigs,
      List<HeaderAttributeConfig> headerAttributeConfigs,
      List<FieldAttributeConfig> fieldAttributeConfigs
  ) {
    this.expressionProcessorConfigs = expressionProcessorConfigs;
    this.headerAttributeConfigs = headerAttributeConfigs;
    this.fieldAttributeConfigs = fieldAttributeConfigs;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues =  super.init();
    expressionVars = ELUtils.parseConstants(
        null, getContext(), Groups.EXPRESSIONS.name(), "constants", Errors.EXPR_01, issues
    );
    expressionVars.addContextVariable(StringELConstants.MEMOIZED, memoizedVars);
    expressionEval = createExpressionEval(getContext());
    for(ExpressionProcessorConfig expressionProcessorConfig : expressionProcessorConfigs) {
      ELUtils.validateExpression(expressionProcessorConfig.expression, getContext(),
        Groups.EXPRESSIONS.name(), "expressionProcessorConfigs", Errors.EXPR_00, issues);
    }

    if(headerAttributeConfigs != null && !headerAttributeConfigs.isEmpty()) {
      headerAttributeEval = createHeaderAttributeEval(getContext());
      for (HeaderAttributeConfig headerAttributeConfig : headerAttributeConfigs) {
        ELUtils.validateExpression(headerAttributeConfig.headerAttributeExpression,
          getContext(), Groups.EXPRESSIONS.name(), "headerAttributeConfigs", Errors.EXPR_00, issues);
      }
    }

    if (fieldAttributeConfigs != null && !fieldAttributeConfigs.isEmpty()) {
      fieldAttributeEval = createFieldAttributeEval(getContext());
      for (FieldAttributeConfig fieldAttributeConfig : fieldAttributeConfigs) {
        ELUtils.validateExpression(fieldAttributeConfig.fieldAttributeExpression,
            getContext(),
            Groups.EXPRESSIONS.name(),
            "fieldAttributeConfigs",
            Errors.EXPR_00, issues
        );
      }
    }

    return issues;
  }

  private ELEval createExpressionEval(ELContext elContext) {
    return elContext.createELEval("expression");
  }

  private ELEval createHeaderAttributeEval(ELContext elContext) {
    return elContext.createELEval("headerAttributeExpression");
  }

  private ELEval createFieldAttributeEval(ELContext elContext) {
    return elContext.createELEval("fieldAttributeExpression");
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    RecordEL.setRecordInContext(expressionVars, record);
    TimeNowEL.setTimeNowInContext(expressionVars, new Date());

    for(ExpressionProcessorConfig expressionProcessorConfig : expressionProcessorConfigs) {
      String fieldToSet = expressionProcessorConfig.fieldToSet;
      if(fieldToSet == null || fieldToSet.isEmpty()) {
        continue;
      }
      Object result;
      try {
        result = expressionEval.eval(expressionVars, expressionProcessorConfig.expression, Object.class);
      } catch (ELEvalException e) {
        throw new OnRecordErrorException(Errors.EXPR_03, expressionProcessorConfig.expression,
                                         record.getHeader().getSourceId(), e.toString(), e);
      }

      Field newField = null;
      // we want to preserve existing type info if we have it iff the result value is null.
      if (result == null && record.has(fieldToSet)) {
        newField = Field.create(record.get(fieldToSet).getType(), null);
      } else {
        // otherwise, deduce type from result, even if it's null (which will result in coercion to string)
        newField = Field.create(FieldUtils.getTypeFromObject(result), result);
      }

      if(FieldRegexUtil.hasWildCards(fieldToSet)) {
        for(String field : FieldRegexUtil.getMatchingFieldPaths(fieldToSet, record.getEscapedFieldPaths())) {
          record.set(field, newField);
        }
      } else {
        if (record.has(fieldToSet)) {
          record.set(fieldToSet, newField);
        } else {
          //A new field will be created only if the parent field exists and supports creation of a new child field.
          //For a new field can be created in the parent field which is a map or if the parent field is an array.
          try {
            record.set(fieldToSet, newField);
          } catch (IllegalArgumentException e) {
            throw new OnRecordErrorException(record, Errors.EXPR_04, record.getHeader().getSourceId(),
                expressionProcessorConfig.fieldToSet, e.toString());
          }
          if (!record.has(fieldToSet)) {
            throw new OnRecordErrorException(Errors.EXPR_02, record.getHeader().getSourceId(),
              expressionProcessorConfig.fieldToSet);
          }
        }
      }
    }

    if(headerAttributeConfigs != null && !headerAttributeConfigs.isEmpty()) {
      for (HeaderAttributeConfig headerAttributeConfig : headerAttributeConfigs) {
        String attributeToSet = headerAttributeConfig.attributeToSet;
        if (attributeToSet == null || attributeToSet.isEmpty()) {
          continue;
        }
        String result;
        try {
          result = headerAttributeEval.eval(expressionVars, headerAttributeConfig.headerAttributeExpression, String.class);
        } catch (ELEvalException e) {
          throw new OnRecordErrorException(Errors.EXPR_03, headerAttributeConfig.headerAttributeExpression,
            record.getHeader().getSourceId(), e.toString(), e);
        }
        record.getHeader().setAttribute(attributeToSet, result);
      }
    }

    if (fieldAttributeConfigs != null && !fieldAttributeConfigs.isEmpty()) {
      for (FieldAttributeConfig fieldAttributeConfig : fieldAttributeConfigs) {
        String attributeToSet = fieldAttributeConfig.attributeToSet;
        if (attributeToSet == null || attributeToSet.isEmpty()) {
          continue;
        }
        Field field = record.get(fieldAttributeConfig.fieldToSet);
        if (field == null) {
          throw new OnRecordErrorException(
              Errors.EXPR_05,
              record.getHeader().getSourceId(),
              fieldAttributeConfig.fieldToSet,
              fieldAttributeConfig.attributeToSet,
              fieldAttributeConfig.fieldAttributeExpression
          );
        }
        String result;
        try {
          result = fieldAttributeEval.eval(
              expressionVars,
              fieldAttributeConfig.fieldAttributeExpression,
              String.class
          );
        } catch (ELEvalException e) {
          throw new OnRecordErrorException(
              Errors.EXPR_03,
              fieldAttributeConfig.fieldAttributeExpression,
              record.getHeader().getSourceId(),
              e.toString(),
              e
          );
        }
        field.setAttribute(fieldAttributeConfig.attributeToSet, result);
      }
    }

    batchMaker.addRecord(record);
  }

}
