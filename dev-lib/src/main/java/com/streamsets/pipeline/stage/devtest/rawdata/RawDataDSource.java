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
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.configurablestage.DSource;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GenerateResourceBundle
@StageDef(
    version = 2,
    label = "Dev Raw Data Source",
    description = "Add Raw data to the source.",
    execution = {ExecutionMode.STANDALONE, ExecutionMode.EDGE},
    icon = "dev.png",
    upgrader = RawDataSourceUpgrader.class,
    onlineHelpRefUrl = "index.html#Pipeline_Design/DevStages.html"
)
@ConfigGroups(value = RawDataSourceGroups.class)
public class RawDataDSource extends DSource {
  private static final Logger LOG = LoggerFactory.getLogger(RawDataDSource.class);


  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Data Format",
    displayPosition = 1,
    group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = "RAW")
  public DataParserFormatConfig dataFormatConfig;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.TEXT,
    mode = ConfigDef.Mode.JSON,
    label = "Raw Data",
    evaluation = ConfigDef.Evaluation.IMPLICIT,
    displayPosition = 20,
    group = "RAW"
  )
  public String rawData;

  @ConfigDef(
    required = true,
    defaultValue = "false",
    type = ConfigDef.Type.BOOLEAN,
    label = "Stop After First Batch",
    displayPosition = 30,
    group = "RAW"
  )
  public boolean stopAfterFirstBatch = false;

  @Override
  protected Source createSource() {
    return new RawDataSource(dataFormat, dataFormatConfig, rawData, stopAfterFirstBatch);
  }
}
