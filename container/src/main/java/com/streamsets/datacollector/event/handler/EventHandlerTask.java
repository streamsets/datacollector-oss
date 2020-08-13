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

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.event.dto.Event;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollectorResult;
import com.streamsets.datacollector.task.Task;

import java.util.Map;

public interface EventHandlerTask extends Task {

  /**
   * Handles a locally originating event (i.e. from the Data Collector itself).  The current use case is for dynamic
   * preview.  It is expected there is a local caller that will make use of the result.
   *
   * @param event the event to handle
   * @param eventType the type of the event
   * @param connections the connection definitions for the pipeline
   * @return an {@link RemoteDataCollectorResult} encapsulating the results of handling the event
   */
  RemoteDataCollectorResult handleLocalEvent(Event event, EventType eventType, Map<String, ConnectionConfiguration> connections);

  /**
   * Handles a remotely originating event (i.e. from the messaging app).
   *
   * @param event the event to handle
   * @param eventType the type of the event
   * @param connections the connection definitions for the pipeline
   * @return an {@link RemoteDataCollectorResult} encapsulating the results of handling the event
   */
  RemoteDataCollectorResult handleRemoteEvent(Event event, EventType eventType, Map<String, ConnectionConfiguration> connections);
}
