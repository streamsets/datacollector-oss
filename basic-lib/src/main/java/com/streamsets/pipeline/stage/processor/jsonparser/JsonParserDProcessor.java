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
package com.streamsets.pipeline.stage.processor.jsonparser;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

@StageDef(
    version = 1,
    label = "JSON Parser",
    description = "Parses a string field with JSON data",
    icon = "json.png",
    upgraderDef = "upgrader/JsonParserDProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_kwz_lg2_zq"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class JsonParserDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field to Parse",
      description = "String field that contains a JSON object",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "JSON"
  )
  @FieldSelectorModel(singleValued = true)
  public String fieldPathToParse;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Ignore Control Characters",
      description = "Use only if required as it impacts reading performance",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "JSON"
  )
  public boolean removeCtrlChars;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Target Field",
      description="Name of the field to set the parsed JSON data to",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "JSON"
  )
  public String parsedFieldPath;

  @Override
  protected Processor createProcessor() {
    return new JsonParserProcessor(fieldPathToParse, removeCtrlChars, parsedFieldPath);
  }
}
