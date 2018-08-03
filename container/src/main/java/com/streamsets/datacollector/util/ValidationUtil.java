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
package com.streamsets.datacollector.util;

import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.Record;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * Various validation utilities for our execution engine (various runners).
 */
public class ValidationUtil {

  private  ValidationUtil() {}

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

  /**
   * Returns true if given snapshot output is usable - e.g. if it make sense to persist.
   */
  public static boolean isSnapshotOutputUsable(List<StageOutput> stagesOutput) {
    // In case that the snapshot actually does not exists
    if(stagesOutput == null) {
      return false;
    }

    // We're looking for at least one output lane that is not empty. In most cases the first stage in the list will
    // be origin that generated some data and hence the loop will terminate fast. In the worst case scenario we will
    // iterate over all stages in attempt to find at least one record in the snapshot.
    for(StageOutput output : stagesOutput) {
      if (CollectionUtils.isNotEmpty(output.getErrorRecords()) ||
          CollectionUtils.isNotEmpty(output.getEventRecords()) ||
          CollectionUtils.isNotEmpty(output.getStageErrors())) {
        return true;
      }
      for(Map.Entry<String, List<Record>> entry : output.getOutput().entrySet()) {
        if(!entry.getValue().isEmpty()) {
          return true;
        }
      }
    }

    return false;
  }
}
