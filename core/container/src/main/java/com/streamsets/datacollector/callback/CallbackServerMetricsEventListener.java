/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.callback;

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsEventListener;

public final class CallbackServerMetricsEventListener extends CallbackServerEventListener implements
    MetricsEventListener {

  public CallbackServerMetricsEventListener(
      String user,
      String name,
      String rev,
      RuntimeInfo runtimeInfo,
      String callbackServerURL,
      String sdcClusterToken,
      String sdcSlaveToken
  ) {
    super(user, name, rev, runtimeInfo, callbackServerURL, sdcClusterToken, sdcSlaveToken);
  }

  @Override
  public void notification(String metrics) {
    callback(CallbackObjectType.METRICS, metrics);
  }
}
