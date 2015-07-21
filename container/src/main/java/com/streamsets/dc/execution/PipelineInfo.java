/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution;

import com.streamsets.pipeline.runner.Pipeline;

public interface PipelineInfo {

  Pipeline getPipeline();

}
