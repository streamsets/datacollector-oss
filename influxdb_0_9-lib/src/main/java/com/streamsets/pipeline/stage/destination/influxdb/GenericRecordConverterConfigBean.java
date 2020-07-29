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
package com.streamsets.pipeline.stage.destination.influxdb;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GenericRecordConverterConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Measurement Field",
      description = "Field whose value will be used as the measurement name.",
      displayPosition = 90,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "recordConverterType^",
      triggeredByValue = "CUSTOM",
      group = "#0"
  )
  @FieldSelectorModel(singleValued = true)
  public String measurementField = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Time Field",
      description = "Field containing a timestamp value. Leave blank to use the current system time.",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "recordConverterType^",
      triggeredByValue = "CUSTOM",
      group = "#0"
  )
  @FieldSelectorModel(singleValued = true)
  public String timeField = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Time Unit",
      description = "Precision of the value in the time field.",
      defaultValue = "MILLISECONDS",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "recordConverterType^",
      triggeredByValue = "CUSTOM",
      group = "#0"
  )
  @ValueChooserModel(TimeUnitChooserValues.class)
  public TimeUnit timeUnit;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Tag Fields",
      description = "Fields which will be used as tag keys and values.",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  @FieldSelectorModel
  public List<String> tagFields = new ArrayList<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Value Fields",
      description = "Fields which will be used as measurement field key and field value pairs.",
      displayPosition = 120,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "recordConverterType^",
      triggeredByValue = "CUSTOM",
      group = "#0"
  )
  @FieldSelectorModel
  public List<String> valueFields = new ArrayList<>();
}
