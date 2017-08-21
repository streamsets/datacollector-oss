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
package com.streamsets.pipeline.stage.origin.coapserver;

import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageException;

import java.util.Map;

public abstract class AbstractCoapServerPushSource<R extends CoapReceiver>
    extends AbstractCoapServerProtoSource<R, PushSource.Context> implements PushSource {

  AbstractCoapServerPushSource(CoapServerConfigs coAPServerConfigs, R receiver) {
    super(coAPServerConfigs, receiver);
  }

  @Override
  public int getNumberOfThreads() {
    return getCoAPServerConfigs().maxConcurrentRequests;
  }

  @Override
  public void produce(Map<String, String> map, int i) throws StageException {
    while (!getContext().isStopped()) {
      dispatchHttpReceiverErrors(100);
    }
  }

}
