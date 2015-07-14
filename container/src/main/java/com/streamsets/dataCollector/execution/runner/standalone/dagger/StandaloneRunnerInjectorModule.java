/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner.standalone.dagger;

import com.streamsets.dataCollector.execution.runner.standalone.StandaloneRunner;
import dagger.Module;

@Module(injects = StandaloneRunner.class, library = true, complete = false)
public class StandaloneRunnerInjectorModule {
}
