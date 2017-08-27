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
package com.streamsets.pipeline.lib.eventhubs;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;

import java.io.IOException;

public class EventHubCommon {

  public final static String CONF_NAME_SPACE = "commonConf.namespaceName";
  private final EventHubConfigBean commonConf;

  public EventHubCommon(EventHubConfigBean commonConf) {
    this.commonConf = commonConf;
  }

  public EventHubClient createEventHubClient() throws IOException, ServiceBusException {
    ConnectionStringBuilder connStr = new ConnectionStringBuilder(
        commonConf.namespaceName,
        commonConf.eventHubName,
        commonConf.sasKeyName,
        commonConf.sasKey
    );
    return EventHubClient.createFromConnectionStringSync(connStr.toString());
  }

}
