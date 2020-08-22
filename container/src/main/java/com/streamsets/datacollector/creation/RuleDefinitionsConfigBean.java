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
package com.streamsets.datacollector.creation;

import com.streamsets.datacollector.config.PipelineRulesGroups;
import com.streamsets.datacollector.config.RuleDefinitionsWebhookConfig;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;

import java.util.Collections;
import java.util.List;

// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
@StageDef(
    version = RuleDefinitionsConfigBean.VERSION,
    label = "Pipeline Rules",
    upgrader = RuleDefinitionsConfigUpgrader.class,
    onlineHelpRefUrl = "not applicable"
)
@ConfigGroups(PipelineRulesGroups.class)
public class RuleDefinitionsConfigBean implements Stage {

  public static final int VERSION = 2;

  @ConfigDef(
      required = false,
      defaultValue = "[]",
      type = ConfigDef.Type.LIST,
      label = "Email IDs",
      description = "Email Addresses",
      displayPosition = 76,
      group = "NOTIFICATIONS"
  )
  public List<String> emailIDs;

  @ConfigDef(
      required = true,
      defaultValue = "[]",
      type = ConfigDef.Type.MODEL,
      label = "Webhooks",
      description = "Webhooks",
      displayPosition = 200,
      group = "NOTIFICATIONS"
  )
  @ListBeanModel
  public List<RuleDefinitionsWebhookConfig> webhookConfigs;

  @Override
  public List<ConfigIssue> init(Info info, Context context) {
    return Collections.emptyList();
  }

  @Override
  public void destroy() {
  }

}
