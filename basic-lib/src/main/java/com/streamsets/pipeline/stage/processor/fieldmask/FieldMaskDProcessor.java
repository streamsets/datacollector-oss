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
package com.streamsets.pipeline.stage.processor.fieldmask;

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version = 1,
    label = "Field Masker",
    description = "Masks field values",
    icon = "mask.png",
    flags = StageBehaviorFlags.PURE_FUNCTION,
    upgraderDef = "upgrader/FieldMaskDProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_vgg_z44_wq"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FieldMaskDProcessor extends DProcessor {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "",
      description = "",
      displayPosition = 10,
      group = "MASKING"
  )
  @ListBeanModel
  public List<FieldMaskConfig> fieldMaskConfigs;

  @Override
  protected Processor createProcessor() {
    return new FieldMaskProcessor(fieldMaskConfigs);
  }

}
