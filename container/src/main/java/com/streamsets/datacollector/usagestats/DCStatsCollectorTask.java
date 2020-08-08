/*
 * Copyright 2020 StreamSets Inc.
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
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

import java.util.Collections;
import java.util.List;

public class DCStatsCollectorTask extends AbstractStatsCollectorTask {

  public DCStatsCollectorTask(
          BuildInfo buildInfo,
          RuntimeInfo runtimeInfo,
          Configuration config,
          SafeScheduledExecutorService executorService,
          SysInfo sysInfo,
          Activation activation) {
    super(buildInfo, runtimeInfo, config, executorService, sysInfo, activation);
  }

  @Override
  protected List<AbstractStatsExtension> provideStatsExtensions() {
    return Collections.emptyList();
  }

  /**
   *
   * @param rawStats raw stats.json or json array of collected stats to report
   * @return
   */
  public boolean reportStats(String rawStats) {
    return reportStats(null, rawStats);
  }
}
