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
package com.streamsets.datacollector.event.handler;

import javax.inject.Inject;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.event.dto.Event;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollectorResult;
import com.streamsets.datacollector.task.AbstractTask;

import java.util.Map;

public class NoOpEventHandlerTask extends AbstractTask implements EventHandlerTask {

  @Inject
  public NoOpEventHandlerTask() {
    super("NO_OP_HANDLER_TASK");
  }

  @Override
  public RemoteDataCollectorResult handleLocalEvent(Event event, EventType eventType, Map<String, ConnectionConfiguration> connections) {
    return RemoteDataCollectorResult.empty();
  }

  @Override
  public RemoteDataCollectorResult handleRemoteEvent(Event event, EventType eventType, Map<String, ConnectionConfiguration> connections) {
    return RemoteDataCollectorResult.empty();
  }
}
