/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.scheduler;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.TimeZoneChooserValues;

public class SchedulerConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "0 0/1 * 1/1 * ? *",
      label = "Cron Schedule",
      description = "Cron expression that defines when to generate a record",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CRON"
  )
  public String cronExpression;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Time Zone",
      description = "The time zone in which to base the schedule",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CRON"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID = "UTC";

}
