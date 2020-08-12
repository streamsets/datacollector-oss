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

package com.streamsets.pipeline.stage.processor.jsongenerator;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

@StageDef(
    version = 1,
    label = "JSON Generator",
    description = "Serializes a field to a string field in JSON format",
    icon = "json.png",
    flags = StageBehaviorFlags.PURE_FUNCTION,
    upgraderDef = "upgrader/JsonGeneratorDProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_kgk_3w1_h1b"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class JsonGeneratorDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Field to Serialize",
      description = "Map or List field to serialize to JSON",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "JSON"
  )
  @FieldSelectorModel(singleValued = true)
  public String fieldPathToSerialize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Target Field",
      description="Name of the field in which to place the serialized JSON string",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "JSON"
  )
  public String outputFieldPath;

  @Override
  protected Processor createProcessor() {
    return new JsonGeneratorProcessor(fieldPathToSerialize, outputFieldPath);
  }
}
