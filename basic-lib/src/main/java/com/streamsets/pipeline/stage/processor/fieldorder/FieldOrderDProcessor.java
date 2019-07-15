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
package com.streamsets.pipeline.stage.processor.fieldorder;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.stage.processor.fieldorder.config.Groups;
import com.streamsets.pipeline.stage.processor.fieldorder.config.OrderConfigBean;

@StageDef(
  version = 1,
  label = "Field Order",
  description = "Order MAP or LIST_MAP fields into LIST_MAP or LIST.",
  icon = "order.png",
  flags = StageBehaviorFlags.PURE_FUNCTION,
    upgraderDef = "upgrader/FieldOrderDProcessor.yaml",
  onlineHelpRefUrl = "index.html?contextID=task_myf_wfv_vy"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FieldOrderDProcessor extends DProcessor {

  @ConfigDefBean
  public OrderConfigBean config;

  @Override
  protected Processor createProcessor() {
    return new FieldOrderProcessor(config);
  }
}
