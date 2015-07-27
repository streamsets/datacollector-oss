/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.util;

import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.el.JvmEL;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.lib.el.StringEL;

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

}
