/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
import java.util.List;
import java.util.Map;

public class ExpressionProcessor extends SingleLaneRecordProcessor {

  private final List<ExpressionProcessorConfig> expressionProcessorConfigs;

  public ExpressionProcessor(
      List<ExpressionProcessorConfig> expressionProcessorConfigs) {
    this.expressionProcessorConfigs = expressionProcessorConfigs;
  }

  private ELEval expressionEval;
  private ELVars variables;

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues =  super.validateConfigs();
    variables = ELUtils.parseConstants(null, getContext(), Groups.EXPRESSIONS.name(), "constants",
      Errors.EXPR_01, issues);
    expressionEval = createExpressionEval(getContext());
    for(ExpressionProcessorConfig expressionProcessorConfig : expressionProcessorConfigs) {
      ELUtils.validateExpression(expressionEval, variables, expressionProcessorConfig.expression, getContext(),
        Groups.EXPRESSIONS.name(), "expressionProcessorConfigs", Errors.EXPR_00,
        Object.class, issues);
    }
    return issues;
  }

  private ELEval createExpressionEval(ELContext elContext) {
    return elContext.createELEval("expression");
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    RecordEL.setRecordInContext(variables, record);
    for(ExpressionProcessorConfig expressionProcessorConfig : expressionProcessorConfigs) {
      String fieldToSet = expressionProcessorConfig.fieldToSet;
      if(fieldToSet == null || fieldToSet.isEmpty()) {
        continue;
      }
      Object result;
      try {
        result = expressionEval.eval(variables, expressionProcessorConfig.expression, Object.class);
      } catch (ELEvalException e) {
        throw new OnRecordErrorException(Errors.EXPR_03, expressionProcessorConfig.expression,
                                         record.getHeader().getSourceId(), e.getMessage(), e);
      }
      Field newField = Field.create(getTypeFromObject(result), result);

      if(FieldRegexUtil.hasWildCards(fieldToSet)) {
        for(String field : FieldRegexUtil.getMatchingFieldPaths(fieldToSet, record)) {
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
    } else if(result instanceof Map) {
      return Field.Type.MAP;
    } else if(result instanceof List) {
      return Field.Type.LIST;
    }
    return Field.Type.STRING;
  }

}
