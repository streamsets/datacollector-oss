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
package com.streamsets.datacollector.event.client.api;

import java.util.List;
import java.util.Map;

import com.streamsets.datacollector.event.dto.Event;
import com.streamsets.datacollector.event.dto.PipelineStatusEvent;
import com.streamsets.datacollector.event.json.ClientEventJson;
import com.streamsets.datacollector.event.json.SDCMetricsJson;
import com.streamsets.datacollector.event.json.ServerEventJson;

public interface EventClient {

  List<ServerEventJson> submit(
    String path,
    Map<String, String> queryParams,
    Map<String, String> headerParams,
    boolean compression,
    List<ClientEventJson> clientEventJson) throws EventException;

  void submit(
      String path,
      Map<String, String> queryParams,
      Map<String, String> headerParams,
      List<SDCMetricsJson> sdcMetricsJsons,
      long retryAttempts);

  /**
   *
   * This API is for sending updates to SCH directly without going through messaging app.
   * The newer SDC's should use this API to send critical events to SCH.
   *
   * @param absoluteTargetUrl - the absolute target URL of the APP
   * @param queryParams - query params for the REST call
   * @param headerParams - header parameters for the REST call
   * @param event - Event to be sent
   * @param retryAttempts - No of attempts before bailing out
   */
  void sendSyncEvents(
      String absoluteTargetUrl,
      Map<String, String> queryParams,
      Map<String, String> headerParams,
      Event event,
      long retryAttempts);

}
