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
package com.streamsets.pipeline.stage.processor.fieldrenamer;

import com.streamsets.pipeline.api.ConfigDefBean;
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
    version=2,
    label="Field Renamer",
    description = "Rename fields",
    icon="edit.png",
    upgrader = FieldRenamerProcessorUpgrader.class,
    upgraderDef = "upgrader/FieldRenamerDProcessor.yaml",
    flags = StageBehaviorFlags.PURE_FUNCTION,
    onlineHelpRefUrl ="index.html?contextID=task_y5g_4hh_ht"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FieldRenamerDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Fields to Rename",
      description = "Fields to rename, and target field names.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RENAME"
  )
  @ListBeanModel
  public List<FieldRenamerConfig> renameMapping;


  @ConfigDefBean
  public FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();

  @Override
  protected Processor createProcessor() {
    return new FieldRenamerProcessor(
        renameMapping,
        errorHandler
    );
  }
}
