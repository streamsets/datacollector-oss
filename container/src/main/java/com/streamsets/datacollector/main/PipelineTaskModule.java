/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.main;

import com.streamsets.datacollector.main.LogConfigurator;
import com.streamsets.datacollector.main.RuntimeInfo;

import dagger.Module;

@Module(injects = {PipelineTask.class, LogConfigurator.class, RuntimeInfo.class},
  library = true, complete = false)
public class PipelineTaskModule {
}
