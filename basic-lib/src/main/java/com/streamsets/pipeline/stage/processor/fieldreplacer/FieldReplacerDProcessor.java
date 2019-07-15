/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.fieldreplacer;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.stage.processor.fieldreplacer.config.ReplacerConfigBean;

@StageDef(
    version=1,
    label="Field Replacer",
    description = "Replaces field values.",
    icon="replacer.png",
    flags = StageBehaviorFlags.PURE_FUNCTION,
    upgraderDef = "upgrader/FieldReplacerDProcessor.yaml",
    onlineHelpRefUrl = "index.html?contextID=task_fk5_kd3_4cb"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FieldReplacerDProcessor extends DProcessor {

  @ConfigDefBean
  public ReplacerConfigBean conf;

  @Override
  protected Processor createProcessor() {
    return new FieldReplacerProcessor(conf);
  }
}
