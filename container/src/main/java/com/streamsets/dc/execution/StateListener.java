/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution;

import java.util.Map;

import com.streamsets.pipeline.runner.PipelineRuntimeException;

public interface StateListener {

  void stateChanged(PipelineStatus pipelineStatus, String message, Map<String, Object> attributes)
    throws PipelineRuntimeException;

}
