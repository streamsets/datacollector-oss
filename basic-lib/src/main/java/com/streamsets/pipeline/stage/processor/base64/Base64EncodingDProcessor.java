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
package com.streamsets.pipeline.stage.processor.base64;

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
    label = "Base64 Field Encoder",
    icon = "base64encoder.png",
    description = "Encodes a Byte Array field into a Base64 encoded Byte Array",
    flags = StageBehaviorFlags.PURE_FUNCTION,
    upgraderDef = "upgrader/Base64EncodingDProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_ekg_ppy_kv"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class Base64EncodingDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Field to Encode",
      description = "Byte Array field that is to be encoded",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "BASE64"
  )
  @FieldSelectorModel(singleValued = true)
  public String originFieldPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Target Field",
      description = "Target field to which encoded Byte Array is to be written to",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "BASE64"
  )
  public String resultFieldPath;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "URL Safe",
      description = "Encode the field so that it can be safely sent in a URL",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "BASE64"
  )
  public boolean urlSafe;

  @Override
  protected Processor createProcessor() {
    return new Base64EncodingProcessor(originFieldPath, resultFieldPath, urlSafe);
  }
}
