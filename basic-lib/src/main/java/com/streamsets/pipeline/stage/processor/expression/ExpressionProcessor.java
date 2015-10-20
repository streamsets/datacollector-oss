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
import com.streamsets.pipeline.lib.util.FieldRegexUtil;

import java.math.BigDecimal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ExpressionProcessor extends SingleLaneRecordProcessor {

  private final List<ExpressionProcessorConfig> expressionProcessorConfigs;
  private final List<HeaderAttributeConfig> headerAttributeConfigs;

  public ExpressionProcessor(
      List<ExpressionProcessorConfig> expressionProcessorConfigs,
      List<HeaderAttributeConfig> headerAttributeConfigs) {
    this.expressionProcessorConfigs = expressionProcessorConfigs;
    this.headerAttributeConfigs = headerAttributeConfigs;
  }

  private ELEval expressionEval;
  private ELVars expressionVars;

  private ELEval headerAttributeEval;

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues =  super.init();
    expressionVars = ELUtils.parseConstants(null, getContext(), Groups.EXPRESSIONS.name(), "constants",
      Errors.EXPR_01, issues);
    expressionEval = createExpressionEval(getContext());
    for(ExpressionProcessorConfig expressionProcessorConfig : expressionProcessorConfigs) {
      ELUtils.validateExpression(expressionEval, expressionVars, expressionProcessorConfig.expression, getContext(),
        Groups.EXPRESSIONS.name(), "expressionProcessorConfigs", Errors.EXPR_00,
        Object.class, issues);
    }

    if(headerAttributeConfigs != null && !headerAttributeConfigs.isEmpty()) {
      headerAttributeEval = createHeaderAttributeEval(getContext());
      for (HeaderAttributeConfig headerAttributeConfig : headerAttributeConfigs) {
        ELUtils.validateExpression(headerAttributeEval, expressionVars, headerAttributeConfig.headerAttributeExpression,
          getContext(), Groups.EXPRESSIONS.name(), "headerAttributeConfigs", Errors.EXPR_00, Object.class, issues);
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

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    RecordEL.setRecordInContext(expressionVars, record);
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
      Field newField = Field.create(getTypeFromObject(result), result);

      if(FieldRegexUtil.hasWildCards(fieldToSet)) {
        for(String field : FieldRegexUtil.getMatchingFieldPaths(fieldToSet, record.getFieldPaths())) {
          record.set(field, newField);
        }
      } else {
        if (record.has(fieldToSet)) {
          record.set(fieldToSet, newField);
        } else {
          //A new field will be created only if the parent field exists and supports creation of a new child field.
          //For a new field can be created in the parent field which is a map or if the parent field is an array.
          record.set(fieldToSet, newField);
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
    batchMaker.addRecord(record);
  }

  private Field.Type getTypeFromObject(Object result) {
    if(result instanceof Double) {
      return Field.Type.DOUBLE;
    } else if(result instanceof Long) {
      return Field.Type.LONG;
    } else if(result instanceof BigDecimal) {
      return Field.Type.DECIMAL;
    } else if(result instanceof Date) {
      return Field.Type.DATE;
    } else if(result instanceof Short) {
      return Field.Type.SHORT;
    } else if(result instanceof Boolean) {
      return Field.Type.BOOLEAN;
    } else if(result instanceof Byte) {
      return Field.Type.BYTE;
    } else if(result instanceof byte[]) {
      return Field.Type.BYTE_ARRAY;
    } else if(result instanceof Character) {
      return Field.Type.CHAR;
    } else if(result instanceof Float) {
      return Field.Type.FLOAT;
    } else if(result instanceof Integer) {
      return Field.Type.INTEGER;
    } else if(result instanceof String) {
      return Field.Type.STRING;
    } else if(result instanceof LinkedHashMap) {
      return Field.Type.LIST_MAP;
    } else if(result instanceof Map) {
      return Field.Type.MAP;
    } else if(result instanceof List) {
      return Field.Type.LIST;
    }
    return Field.Type.STRING;
  }

}
