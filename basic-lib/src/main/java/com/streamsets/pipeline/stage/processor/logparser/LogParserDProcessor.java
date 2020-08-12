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
package com.streamsets.pipeline.stage.processor.logparser;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.log.LogParserService;

@StageDef(
    version=2,
    label="Log Parser",
    description = "Parses a string field which contains a Log line",
    icon="logparser.png",
    onlineHelpRefUrl ="index.html?contextID=task_jm1_b4w_fs",
    upgrader = LogParserUpgrader.class,
    upgraderDef = "upgrader/LogParserDProcessor.yaml",
    services = @ServiceDependency(
        service = LogParserService.class
    )
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class LogParserDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "/text",
      label = "Field to Parse",
      description = "String field that contains a LOG line",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "LOG"
  )
  @FieldSelectorModel(singleValued = true)
  public String fieldPathToParse;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "/",
    label = "New Parsed Field",
    description="Name of the new field to set the parsed JSON data",
    displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
    group = "LOG"
  )
  public String parsedFieldPath;

  @Override
  protected Processor createProcessor() {
    return new LogParserProcessor(fieldPathToParse, parsedFieldPath);
  }
}
