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
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import com.streamsets.pipeline.el.ELUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class ExpressionProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(ExpressionProcessor.class);

  private final List<ExpressionProcessorConfig> expressionProcessorConfigs;
  private final Map<String, ?> constants;

  public ExpressionProcessor(
      List<ExpressionProcessorConfig> expressionProcessorConfigs, Map<String, ?> constants) {
    this.expressionProcessorConfigs = expressionProcessorConfigs;
    this.constants = constants;
  }

  private ELEvaluator elEvaluator;
  private ELEvaluator.Variables variables;

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues =  super.validateConfigs();
    variables = ELUtils.parseConstants(constants, getContext(), Groups.EXPRESSIONS.name(), "constants",
                                       Errors.EXPR_01, issues);
    elEvaluator = new ELEvaluator();
    ELRecordSupport.registerRecordFunctions(elEvaluator);
    ELStringSupport.registerStringFunctions(elEvaluator);
    for(ExpressionProcessorConfig expressionProcessorConfig : expressionProcessorConfigs) {
      ELUtils.validateExpression(elEvaluator, variables, expressionProcessorConfig.expression, getContext(),
                                 Groups.EXPRESSIONS.name(), "expressionProcessorConfigs", Errors.EXPR_00,
                                 Object.class, issues);
    }
    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    ELRecordSupport.setRecordInContext(variables, record);
    for(ExpressionProcessorConfig expressionProcessorConfig : expressionProcessorConfigs) {
      String fieldToSet = expressionProcessorConfig.fieldToSet;
      if(fieldToSet == null || fieldToSet.isEmpty()) {
        continue;
      }
      Object result;
      try {
        result = elEvaluator.eval(variables, expressionProcessorConfig.expression);
      } catch (ELException e) {
        throw new OnRecordErrorException(Errors.EXPR_00, expressionProcessorConfig.expression, e.getMessage(), e);
      }
      Field newField = Field.create(getTypeFromObject(result), result);
      if(record.has(expressionProcessorConfig.fieldToSet)) {
        LOG.debug("Replacing existing field '{}' with value '{}'", expressionProcessorConfig.fieldToSet, result);
        record.set(expressionProcessorConfig.fieldToSet, newField);
      } else {
        LOG.debug("Creating new field '{}' with value '{}'", expressionProcessorConfig.fieldToSet, result);
        //A new field will be created only if the parent field exists and supports creation of a new child field.
        //For a new field can be created in the parent field which is a map or if the parent field is an array.
        record.set(expressionProcessorConfig.fieldToSet, newField);
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
      return Field.Type.BYTE;
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
