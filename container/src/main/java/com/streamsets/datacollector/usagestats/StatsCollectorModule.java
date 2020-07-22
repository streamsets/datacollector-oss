/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.usagestats;

import com.streamsets.datacollector.activation.Activation;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.SysInfo;
import com.streamsets.datacollector.util.SysInfoModule;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import dagger.Module;
import dagger.Provides;

import javax.inject.Named;
import javax.inject.Singleton;

@Module(
    library = true,
    complete = false,
    injects = { StatsCollector.class },
    includes = { SysInfoModule.class }
    )
public class StatsCollectorModule {

  @Provides
  @Singleton
  public StatsCollector provideStatsCollector(
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      Configuration config,
      @Named("runnerExecutor") SafeScheduledExecutorService executorService,
      SysInfo sysInfo,
      Activation activation
  ) {
    return new DCStatsCollectorTask(buildInfo, runtimeInfo, config, executorService, sysInfo, activation);
  }

}
