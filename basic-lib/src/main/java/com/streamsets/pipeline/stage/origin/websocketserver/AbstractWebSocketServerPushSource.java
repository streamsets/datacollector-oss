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
package com.streamsets.pipeline.stage.origin.websocketserver;

import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.http.HttpConfigs;

import java.util.Map;

public abstract class AbstractWebSocketServerPushSource<R extends WebSocketReceiver>
    extends AbstractWebSocketServerProtoSource<R, PushSource.Context> implements PushSource {

  public AbstractWebSocketServerPushSource(HttpConfigs httpConfigs, R receiver) {
    super(httpConfigs, receiver);
  }

  @Override
  public int getNumberOfThreads() {
    return getHttpConfigs().getMaxConcurrentRequests();
  }

  @Override
  public void produce(Map<String, String> map, int i) throws StageException {
    while (!getContext().isStopped()) {
      dispatchHttpReceiverErrors(100);
    }
  }

}
