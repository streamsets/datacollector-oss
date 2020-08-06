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
package com.streamsets.pipeline.stage.processor.http;

import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.lib.el.VaultEL;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.http.DataFormatChooserValues;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.JerseyClientConfigBean;
import com.streamsets.pipeline.stage.common.MissingValuesBehavior;
import com.streamsets.pipeline.stage.common.MissingValuesBehaviorChooserValues;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.common.MultipleValuesBehaviorChooserValues;
import com.streamsets.pipeline.stage.origin.http.PaginationConfigBean;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.http.HttpStatusResponseActionConfigBean;
import com.streamsets.pipeline.stage.origin.http.HttpTimeoutResponseActionConfigBean;
import com.streamsets.pipeline.stage.origin.http.ResponseAction;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.pipeline.stage.util.http.HttpStageUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bean specifying the configuration for an HttpProcessor instance.
 */
public class HttpProcessorConfig {
  @ConfigDefBean(groups = "HTTP")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDefBean(groups = "HTTP")
  public BasicConfig basic = new BasicConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Output Field",
      description = "Field in which to place the result of the HTTP request",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP"
  )
  @FieldSelectorModel(singleValued = true)
  public String outputField;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Header Output Location",
      description = "Field in which to place the result of the HTTP request",
      defaultValue = "HEADER",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  @ValueChooserModel(HeaderOutputLocationChooserValues.class)
  public HeaderOutputLocation headerOutputLocation;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Header Attribute Prefix",
      description = "A prefix to add to record header attributes in the response",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP",
      dependsOn = "headerOutputLocation",
      triggeredByValue = "HEADER"
  )
  public String headerAttributePrefix = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Header Output Field",
      description = "Field in which to place the HTTP response headers.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP",
      dependsOn = "headerOutputLocation",
      triggeredByValue = "FIELD"
  )
  @FieldSelectorModel(singleValued = true)
  public String headerOutputField;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      description = "Data Format of the response. Response will be parsed before being placed in the record.",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Resource URL",
      description = "The HTTP resource URL",
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 1,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP"
  )
  public String resourceUrl = "";

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
      description = "HTTP method to send",
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
      description = "Expression used to determine the HTTP method to use",
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
      description = "Data that should be included as a part of the request",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.BASIC,
      lines = 2,
      dependsOn = "httpMethod",
      elDefs = {RecordEL.class, VaultEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      triggeredByValue = { "POST", "PUT", "DELETE", "PATCH", "EXPRESSION" },
      group = "HTTP"
  )
  public String requestBody = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Default Request Content Type",
      defaultValue = HttpStageUtil.DEFAULT_CONTENT_TYPE,
      description = "Content-Type header to be sent with the request; used if that header is not already present",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "httpMethod",
      elDefs = {RecordEL.class, VaultEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      triggeredByValue = { "POST", "PUT", "DELETE", "PATCH", "EXPRESSION" },
      group = "HTTP"
  )
  public String defaultRequestContentType = HttpStageUtil.DEFAULT_CONTENT_TYPE;

  @ConfigDefBean
  public JerseyClientConfigBean client = new JerseyClientConfigBean();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      label = "Rate Limit (ms)",
      defaultValue = "0",
      description = "Time between requests (in ms, 0 for unlimited). Useful for rate-limited APIs.",
      displayPosition = 160,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  public int rateLimit;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Request Time (sec)",
      defaultValue = "60",
      description = "Maximum time to wait for each request completion.",
      displayPosition = 999,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  public long maxRequestCompletionSecs = 60L;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Missing Values Behavior",
    description = "How to handle missing values when no default value is defined.",
    defaultValue = "PASS_RECORD_ON",
    displayPosition = 11,
    group = "HTTP"
  )
  @ValueChooserModel(MissingValuesBehaviorChooserValues.class)
  public MissingValuesBehavior missingValuesBehavior = MissingValuesBehavior.DEFAULT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Multiple Values Behavior",
      description = "How to handle multiple values produced by the parser",
      defaultValue = "FIRST_ONLY",
      displayPosition = 12,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP"
  )
  @ValueChooserModel(MultipleValuesBehaviorChooserValues.class)
  public MultipleValuesBehavior multipleValuesBehavior = MultipleValuesBehavior.DEFAULT;

  @ConfigDefBean(groups = "PAGINATION")
  public PaginationConfigBean pagination = new PaginationConfigBean();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Per-Status Actions",
      description = "List of actions to take for specific response statuses.",
      displayPosition = 1200,
      group = "HTTP"
  )
  @ListBeanModel
  public List<HttpStatusResponseActionConfigBean> responseStatusActionConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Records for Remaining Statuses",
      description = "Produces records for all HTTP status codes not listed in Per-Status Actions.",
      defaultValue = "false",
      displayPosition = 1201,
      group = "HTTP"
  )
  public boolean propagateAllHttpResponses = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Error Response Body Field",
      description = "Field to store the error response body after performing per-status actions",
      defaultValue = "outErrorBody",
      displayPosition = 1202,
      dependsOn = "propagateAllHttpResponses",
      triggeredByValue = "true",
      group = "HTTP"
  )
  public String errorResponseField = "outErrorBody";

  @ConfigDefBean(groups = "TIMEOUT")
  public HttpTimeoutResponseActionConfigBean responseTimeoutActionConfig =
      new HttpTimeoutResponseActionConfigBean(0, ResponseAction.RETRY_IMMEDIATELY);


  public void init(Stage.Context context, String group, String prefix, List<Stage.ConfigIssue> issues) {
    client.init(context, group, prefix + "client", issues);
  }


}
