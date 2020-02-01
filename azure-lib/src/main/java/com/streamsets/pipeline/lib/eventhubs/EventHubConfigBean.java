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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.credential.CredentialValue;

public class EventHubConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Namespace Name",
      defaultValue = "",
      description = "Namespace that contains the event hub",
      displayPosition = 10,
      group = "EVENT_HUB"
  )
  public String namespaceName = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Event Hub Name",
      defaultValue = "",
      description = "",
      displayPosition = 20,
      group = "EVENT_HUB"
  )
  public String eventHubName = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Shared Access Policy Name",
      defaultValue = "",
      description = "Name of a shared access policy associated with the namespace",
      displayPosition = 30,
      group = "EVENT_HUB"
  )
  public String sasKeyName = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Connection String Key",
      defaultValue = "",
      description = "One of the connection string key values associated with the policy",
      displayPosition = 40,
      group = "EVENT_HUB"
  )
  public CredentialValue sasKey = () -> "";
}
