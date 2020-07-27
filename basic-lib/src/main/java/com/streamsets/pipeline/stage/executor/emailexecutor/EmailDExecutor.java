/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.executor.emailexecutor;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DExecutor;

import java.util.List;

@StageDef(
    version=1,
    label="Email",
    description = "Sends emails upon receipt of specific events",
    icon="envelope.png",
    upgraderDef = "upgrader/EmailDExecutor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_pyx_tfp_qz"
)

@ConfigGroups(Groups.class)
@PipelineLifecycleStage
@GenerateResourceBundle
public class EmailDExecutor extends DExecutor {
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Email Configuration",
      description = "Configure Email Messages",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "EMAILS"
  )
  @ListBeanModel
  public List<EmailConfig> conf;

  @Override
  protected Executor createExecutor() {
    return new EmailExecutor(conf);
  }
}
