/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.runner.cluster.dagger;

import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.runner.cluster.ClusterRunner;

import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = ClusterRunner.class, library = true, complete = false)
public class ClusterRunnerInjectorModule {
}
