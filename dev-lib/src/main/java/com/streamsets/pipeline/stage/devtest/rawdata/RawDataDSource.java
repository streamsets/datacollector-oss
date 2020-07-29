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
package com.streamsets.pipeline.stage.devtest.rawdata;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSource;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;

@GenerateResourceBundle
@StageDef(
    version = 3,
    label = "Dev Raw Data Source",
    description = "Add Raw data to the source.",
    execution = {ExecutionMode.STANDALONE, ExecutionMode.EDGE},
    icon = "dev.png",
    upgrader = RawDataSourceUpgrader.class,
    upgraderDef = "upgrader/RawDataDSource.yaml",
    producesEvents = true,
    recordsByRef = true,
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Pipeline_Design/DevStages.html",
    services = @ServiceDependency(
        service = DataFormatParserService.class,
        configuration = {
            @ServiceConfiguration(name = "displayFormats", value = "DELIMITED,JSON,LOG,SDC_JSON,TEXT,XML"),
            @ServiceConfiguration(name = "dataFormat", value = "JSON")
        }
    )
)
@ConfigGroups(value = RawDataSourceGroups.class)
public class RawDataDSource extends DSource {

  private static final String DEFAULT_RAW_DATA =
      "{\n" +
      "  \"f1\": \"abc\",\n" +
      "  \"f2\": \"xyz\",\n" +
      "  \"f3\": \"lmn\"\n" +
      "}";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.JSON,
      label = "Raw Data",
      defaultValue = DEFAULT_RAW_DATA,
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      displayPosition = 1,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RAW"
  )
  public String rawData;


  @ConfigDef(
      required = true,
      defaultValue = "false",
      type = ConfigDef.Type.BOOLEAN,
      label = "Stop After First Batch",
      displayPosition = 2,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RAW"
  )
  public boolean stopAfterFirstBatch = false;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.JSON,
      label = "Event Data",
      defaultValue = "", // By default empty body and thus "no-op" (no events generated)
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "EVENT"
  )
  public String eventData;

  @Override
  protected Source createSource() {
    return new RawDataSource(rawData, eventData, stopAfterFirstBatch);
  }
}
