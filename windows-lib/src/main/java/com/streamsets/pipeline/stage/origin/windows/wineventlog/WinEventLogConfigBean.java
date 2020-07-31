/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.windows.wineventlog;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

public class WinEventLogConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "PULL",
      label = "Subscription Mode",
      description = "Subscription Mode",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "readerAPIType^",
      triggeredByValue = {"WINDOWS_EVENT_LOG"},
      group = "#0"
  )
  @ValueChooserModel(SubscriptionModeChooserValues.class)
  public SubscriptionMode subscriptionMode = SubscriptionMode.PULL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "60",
      label = "Maximum Wait Time (secs)",
      description = "Maximum wait time to generate batch",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      dependsOn = "readerAPIType^",
      triggeredByValue = {"WINDOWS_EVENT_LOG"},
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int maxWaitTimeSecs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ON_ERROR",
      label = "Populate Raw Event XML",
      description = "Strategy for populating raw event XML",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "readerAPIType^",
      triggeredByValue = {"WINDOWS_EVENT_LOG"},
      group = "#0"
  )
  @ValueChooserModel(RawEventPopulationStrategyChooserValues.class)
  public RawEventPopulationStrategy rawEventPopulationStrategy = RawEventPopulationStrategy.ON_ERROR;
}
