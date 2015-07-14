/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner.cluster.dagger;

import com.streamsets.dataCollector.execution.runner.cluster.ClusterRunner;
import dagger.Module;

@Module(injects = ClusterRunner.class, library = true, complete = false)
public class ClusterRunnerInjectorModule {
}
