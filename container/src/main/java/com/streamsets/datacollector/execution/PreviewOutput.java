/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution;

import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.validation.Issues;

import java.util.List;

public interface PreviewOutput {

  public PreviewStatus getStatus();

  public Issues getIssues();

  public List<List<StageOutput>> getOutput();

  public String getMessage();
}
