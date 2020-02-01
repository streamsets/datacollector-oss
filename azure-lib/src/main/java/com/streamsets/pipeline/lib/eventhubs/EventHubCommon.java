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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class EventHubCommon {

  public final static String CONF_NAME_SPACE = "commonConf.namespaceName";
  private final EventHubConfigBean commonConf;

  public EventHubCommon(EventHubConfigBean commonConf) {
    this.commonConf = commonConf;
  }

  public EventHubClient createEventHubClient(String threadNamePattern) throws IOException, EventHubException {
    final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
        .setNamespaceName(commonConf.namespaceName)
        .setEventHubName(commonConf.eventHubName)
        .setSasKey(commonConf.sasKey.get())
        .setSasKeyName(commonConf.sasKeyName);


    final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(threadNamePattern).build()
    );

    return EventHubClient.createSync(connStr.toString(), scheduledExecutorService);
  }

}
