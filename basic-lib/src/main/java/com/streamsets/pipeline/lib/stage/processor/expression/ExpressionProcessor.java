/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.expression;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.el.ELBasicSupport;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import com.streamsets.pipeline.el.ELUtils;
import com.streamsets.pipeline.lib.util.StageLibError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

@GenerateResourceBundle
@StageDef( version="1.0.0", label="Expression Processor", icon="expression.png")
public class ExpressionProcessor extends SingleLaneRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(ExpressionProcessor.class);

  @ConfigDef(label = "Expression Configuration", required = false, type = ConfigDef.Type.MODEL, defaultValue="",
    description="Fields that must be set to the value returned by evaluating the expression")
  @ComplexField
  public List<ExpressionProcessorConfig> expressionProcessorConfigs;

  @ConfigDef(required = true,
    type = ConfigDef.Type.MAP,
    label = "Constants for expressions",
    description = "Defines constant values available to all expressions")
  public Map<String, ?> constants;

  private ELEvaluator elEvaluator;
  private ELEvaluator.Variables variables;

  @Override
  protected void init() throws StageException {
    super.init();
    variables = ELUtils.parseConstants(constants);
    elEvaluator = new ELEvaluator();
    ELBasicSupport.registerBasicFunctions(elEvaluator);
    ELRecordSupport.registerRecordFunctions(elEvaluator);
    ELStringSupport.registerStringFunctions(elEvaluator);
    validateExpressions();
    LOG.debug("Expressions passed validation");
  }

  private void validateExpressions() throws StageException {
    Record record = new Record(){
      @Override
      public Header getHeader() {
        return null;
      }

      @Override
      public Field get() {
        return null;
      }

      @Override
      public Field set(Field field) {
        return null;
      }

      @Override
      public Field get(String fieldPath) {
        return null;
      }

      @Override
      public Field delete(String fieldPath) {
        return null;
      }

      @Override
      public boolean has(String fieldPath) {
        return false;
      }

      @Override
      public Set<String> getFieldPaths() {
        return null;
      }

      @Override
      public Field set(String fieldPath, Field newField) {
        return null;
      }
    };

    ELRecordSupport.setRecordInContext(variables, record);
    for(ExpressionProcessorConfig expressionProcessorConfig : expressionProcessorConfigs) {
      expressionProcessorConfig.expression = "${" + expressionProcessorConfig.expression + "}";
      try {
        elEvaluator.eval(variables, expressionProcessorConfig.expression);
      } catch (ELException ex) {
        LOG.error(StageLibError.LIB_0600.getMessage(), expressionProcessorConfig.expression, ex.getMessage());
        throw new StageException(StageLibError.LIB_0600, expressionProcessorConfig.expression, ex.getMessage(), ex);
      }
    }
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    ELRecordSupport.setRecordInContext(variables, record);
    for(ExpressionProcessorConfig expressionProcessorConfig : expressionProcessorConfigs) {
      Object result;
      try {
        result = elEvaluator.eval(variables, expressionProcessorConfig.expression);
      } catch (ELException e) {
        LOG.error(StageLibError.LIB_0600.getMessage(), expressionProcessorConfig.expression, e.getMessage());
        throw new StageException(StageLibError.LIB_0600, expressionProcessorConfig.expression, e.getMessage(), e);
      }
      Field newField = Field.create(getTypeFromObject(result), result);
      if(record.has(expressionProcessorConfig.fieldToSet)) {
        LOG.debug("Replacing existing field '{}' with value '{}'", expressionProcessorConfig.fieldToSet, result);
        record.set(expressionProcessorConfig.fieldToSet, newField);
      } else {
        LOG.debug("Creating new field '{}' with value '{}'", expressionProcessorConfig.fieldToSet, result);
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

  public static class ExpressionProcessorConfig {

    @ConfigDef(label = "Field", required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      description = "Field to set")
    public String fieldToSet;

    @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "Expression",
      description = "Expression which must be evaluated to generate a value for the field",
      defaultValue = "")
    public String expression;

  }
}
