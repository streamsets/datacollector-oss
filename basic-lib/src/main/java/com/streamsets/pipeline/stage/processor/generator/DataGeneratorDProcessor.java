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
package com.streamsets.pipeline.stage.processor.generator;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;

@StageDef(
  version=1,
  label="Data Generator",
  description = "Serializes records to various different data formats.",
  icon="coding.png",
  flags = StageBehaviorFlags.PURE_FUNCTION,
  services = @ServiceDependency(
    service = DataFormatGeneratorService.class,
    configuration = {
      @ServiceConfiguration(name = "displayFormats", value = "AVRO,BINARY,DELIMITED,JSON,PROTOBUF,TEXT,SDC_JSON,XML")
    }
  ),
  upgraderDef = "upgrader/DataGeneratorDProcessor.yaml",
  onlineHelpRefUrl = "index.html?contextID=task_aw1_hq4_3fb"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class DataGeneratorDProcessor extends DProcessor {

  @ConfigDefBean
  public DataGeneratorConfig config;

  @Override
  protected Processor createProcessor() {
    return new DataGeneratorProcessor(config);
  }
}
