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
package com.streamsets.pipeline.stage.processor.schemagen;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.stage.processor.schemagen.config.Groups;
import com.streamsets.pipeline.stage.processor.schemagen.config.SchemaGeneratorConfig;

@StageDef(
  version = 1,
  label = "Schema Generator",
  description = "Generate schema based on incoming records.",
  icon = "schemagen.png",
  flags = StageBehaviorFlags.PURE_FUNCTION,
  upgraderDef = "upgrader/SchemaGeneratorDProcessor.yaml",
  onlineHelpRefUrl = "index.html?contextID=task_szj_ms3_x1b"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class SchemaGeneratorDProcessor extends DProcessor {

  @ConfigDefBean
  public SchemaGeneratorConfig config;

  @Override
  protected Processor createProcessor() {
    return new SchemaGeneratorProcessor(config);
  }
}
