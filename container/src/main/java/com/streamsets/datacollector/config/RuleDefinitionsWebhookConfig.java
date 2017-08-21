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
package com.streamsets.datacollector.config;

import com.streamsets.pipeline.api.ConfigDef;

public class RuleDefinitionsWebhookConfig extends WebhookCommonConfig {
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      label = "Payload",
      defaultValue = "{\n  \"text\" : \"{{ALERT_TEXT}}. The alert, {{ALERT_NAME}},  was triggered for pipeline " +
          "'{{PIPELINE_TITLE}}'. \\n The threshold of {{ALERT_VALUE}} records, for the following condition was " +
          "reached at {{TIME}}: {{ALERT_CONDITION}} \\n <{{PIPELINE_URL}}|Click here> for details!\"\n}",
      description = "Data that should be included as a part of the Webhook request",
      displayPosition = 240,
      lines = 2,
      dependsOn = "httpMethod",
      triggeredByValue = { "POST", "PUT", "DELETE" },
      group = "WEBHOOK",
      mode = ConfigDef.Mode.JSON
  )
  public String payload = "";
}
