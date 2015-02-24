/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ELUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ELUtils.class);

  public static ELEvaluator.Variables parseConstants(Map<String,?> constants, Stage.Context context, String group,
      String config, ErrorCode err, List<Stage.ConfigIssue> issues) {
    ELEvaluator.Variables variables = new ELEvaluator.Variables();
    if (constants != null) {
      for (Map.Entry<String, ?> entry : constants.entrySet()) {
        try {
          variables.addVariable(entry.getKey(), entry.getValue());
        } catch (Exception ex) {
          issues.add(context.createConfigIssue(group, config, err, constants, ex.getMessage(), ex));
        }
        LOG.debug("Constant: {}='{}'", entry.getKey(), entry.getValue());
      }
    }
    return variables;
  }

  public static void validateExpression(ELEvaluator elEvaluator, ELEvaluator.Variables variables, String expression,
      Processor.Context context, String group, String config, ErrorCode err, Class type, List<Stage.ConfigIssue> issues)
      {
    ELRecordSupport.setRecordInContext(variables, context.createRecord("forValidation"));
    try {
      elEvaluator.eval(variables, expression, type);
    } catch (Exception ex) {
      issues.add(context.createConfigIssue(group, config, err, expression, ex.getMessage(), ex));
    }
  }
}
