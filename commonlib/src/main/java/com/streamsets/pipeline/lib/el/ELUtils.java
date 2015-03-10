/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;

import java.util.List;
import java.util.Map;

public class ELUtils {

  public static ELVars parseConstants(Map<String,?> constants, Stage.Context context, String group,
      String config, ErrorCode err, List<Stage.ConfigIssue> issues) {
    ELVars variables = context.createELVars();
    if (constants != null) {
      for (Map.Entry<String, ?> entry : constants.entrySet()) {
        try {
          variables.addVariable(entry.getKey(), entry.getValue());
        } catch (Exception ex) {
          issues.add(context.createConfigIssue(group, config, err, constants, ex.getMessage(), ex));
        }
      }
    }
    return variables;
  }

  public static void validateExpression(ELEval elEvaluator, ELVars variables, String expression,
      Stage.Context context, String group, String config, ErrorCode err, Class<?> type, List<Stage.ConfigIssue> issues)
  {
    RecordEL.setRecordInContext(variables, context.createRecord("forValidation"));
    try {
      context.parseEL(expression);
      elEvaluator.eval(variables, expression, type);
    } catch (Exception ex) {
      issues.add(context.createConfigIssue(group, config, err, expression, ex.getMessage(), ex));
    }
  }

}
