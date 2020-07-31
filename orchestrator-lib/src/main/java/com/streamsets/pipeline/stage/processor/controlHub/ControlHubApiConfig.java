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
package com.streamsets.pipeline.stage.processor.controlHub;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.el.VaultEL;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bean specifying the configuration for an HttpProcessor instance.
 */
public class ControlHubApiConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "https://cloud.streamsets.com/security/rest/v1/currentUser",
      label = "Control Hub API URL",
      description = "URL for the Control Hub API",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP",
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String baseUrl = "https://cloud.streamsets.com/security/rest/v1/currentUser";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Output Field",
      description = "Field to store the response",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP"
  )
  @FieldSelectorModel(singleValued = true)
  public String outputField;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Headers",
      description = "Headers to include in the request",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      elDefs = {RecordEL.class, VaultEL.class},
      group = "HTTP"
  )
  public Map<String, String> headers = new HashMap<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "HTTP Method",
      defaultValue = "GET",
      description = "HTTP method for the request",
      elDefs = RecordEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP"
  )
  @ValueChooserModel(HttpMethodChooserValues.class)
  public HttpMethod httpMethod = HttpMethod.GET;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "HTTP Method Expression",
      description = "Expression that evaluates to a valid HTTP method",
      displayPosition = 90,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "httpMethod",
      elDefs = RecordEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      triggeredByValue = { "EXPRESSION" },
      group = "HTTP"
  )
  public String methodExpression = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      label = "Request Data",
      description = "Data to include with the request",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.BASIC,
      lines = 2,
      dependsOn = "httpMethod",
      elDefs = {RecordEL.class, VaultEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      triggeredByValue = { "POST", "PUT", "DELETE", "EXPRESSION" },
      group = "HTTP"
  )
  public String requestBody = "";


  @ConfigDefBean
  public HttpClientConfigBean client = new HttpClientConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Request Time (sec)",
      defaultValue = "60",
      description = "Maximum number of seconds to wait for a request to complete",
      displayPosition = 999,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  public long maxRequestCompletionSecs = 60L;

  public void init(Stage.Context context, String group, String prefix, List<Stage.ConfigIssue> issues) {
    client.init(context, group, prefix + "client", issues);
  }
}
