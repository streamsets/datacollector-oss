/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution;

import com.streamsets.pipeline.api.ExecutionMode;

import java.util.Map;

public interface PipelineState {

  public String getUser();

  public String getName();

  public String getRev();

  public long getTimeStamp();

  public PipelineStatus getStatus();

  public String getMessage();

  public Map<String, Object> getAttributes();

  public ExecutionMode getExecutionMode();

}
