/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.manager;

import com.streamsets.datacollector.execution.Runner;
import com.streamsets.pipeline.api.ExecutionMode;

import dagger.ObjectGraph;

/**
 * Implementation of this interface provides instances of Runner.
 */
public interface RunnerProvider {

  public Runner createRunner( String user, String name, String rev, ObjectGraph objectGraph,
                              ExecutionMode executionMode);
}
