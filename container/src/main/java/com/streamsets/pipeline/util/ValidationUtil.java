/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.creation.PipelineConfigBean;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELVariables;
import com.streamsets.pipeline.el.JvmEL;
import com.streamsets.pipeline.el.RuntimeEL;
import com.streamsets.pipeline.lib.el.StringEL;
import com.streamsets.pipeline.validation.Issue;
import com.streamsets.pipeline.validation.Issues;

import java.util.List;
import java.util.Map;

public class ValidationUtil {

  public static String getFirstIssueAsString(String name, Issues issues) {
    StringBuilder sb = new StringBuilder();
    if(issues.getPipelineIssues().size() > 0) {
      sb.append("[").append(name).append("] ").append(issues.getPipelineIssues().get(0).getMessage());
    } else if (issues.getStageIssues().entrySet().size() > 0) {
      Map.Entry<String, List<Issue>> e = issues.getStageIssues().entrySet().iterator().next();
      sb.append("[").append(e.getKey()).append("] ").append(e.getValue().get(0).getMessage());
    }
    sb.append("...");
    return sb.toString();
  }

  public static long evaluateMemoryLimit(String memoryLimitString, Map<String, Object> constants)
    throws ELEvalException {
    ELEvaluator memConfigEval = ElUtil.createElEval(PipelineConfigBean.MEMORY_LIMIT_CONFIG, constants, RuntimeEL.class,
      StringEL.class, JvmEL.class);
    long memoryLimit = memConfigEval.evaluate(new ELVariables(constants), memoryLimitString, Long.class);
    return memoryLimit;
  }
}
