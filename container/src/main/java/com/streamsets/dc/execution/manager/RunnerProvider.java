/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.manager;

import com.streamsets.dc.execution.Runner;
import com.streamsets.pipeline.creation.PipelineConfigBean;
import dagger.ObjectGraph;

/**
 * Implementation of this interface provides instances of Runner.
 */
public interface RunnerProvider {

  public Runner createRunner( String user, String name, String rev, PipelineConfigBean pipelineConfigBean,
                              ObjectGraph objectGraph);
}
