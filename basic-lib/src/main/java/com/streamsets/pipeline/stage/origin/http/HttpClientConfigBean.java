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
package com.streamsets.pipeline.stage.origin.http;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.el.VaultEL;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.DataFormatChooserValues;
import com.streamsets.pipeline.lib.http.Errors;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.JerseyClientConfigBean;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.pipeline.stage.util.http.HttpStageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.stage.origin.http.HttpClientSource.START_AT;

public class HttpClientConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(HttpClientConfigBean.class);

  @ConfigDefBean(groups = "HTTP")
  public BasicConfig basic = new BasicConfig();

  @ConfigDefBean(groups = "HTTP")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Resource URL",
      defaultValue = "https://stream.twitter.com/1.1/statuses/sample.json",
      description = "Specify the HTTP resource URL",
          elDefs = {TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 10,
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
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      elDefs = VaultEL.class,
      group = "HTTP"
  )
  public Map<String, String> headers = new HashMap<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "HTTP Method",
      defaultValue = "GET",
      description = "HTTP method to send",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP"
  )
  @ValueChooserModel(HttpMethodChooserValues.class)
  public HttpMethod httpMethod = HttpMethod.GET;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Body Time Zone",
      description = "Time zone to use for request body evaluation (if time or time now ELs are used)",
      displayPosition = 35,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID = "UTC";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      label = "Request Body",
      description = "Data that should be included as the body of the request",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      elDefs = {VaultEL.class, TimeEL.class, TimeNowEL.class},
      lines = 2,
      dependsOn = "httpMethod",
      triggeredByValue = { "POST", "PUT", "DELETE" },
      group = "HTTP"
  )
  public String requestBody = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Default Request Content Type",
      defaultValue = HttpStageUtil.DEFAULT_CONTENT_TYPE,
      description = "Content-Type header to be sent with the request; used if that header is not already present",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "httpMethod",
      elDefs = VaultEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      triggeredByValue = { "POST", "PUT", "DELETE" },
      group = "HTTP"
  )
  public String defaultRequestContentType = HttpStageUtil.DEFAULT_CONTENT_TYPE;

  @ConfigDefBean
  public JerseyClientConfigBean client = new JerseyClientConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Mode",
      defaultValue = "STREAMING",
      displayPosition = 25,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP"
  )
  @ValueChooserModel(HttpClientModeChooserValues.class)
  public HttpClientMode httpMode = HttpClientMode.STREAMING;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Polling Interval (ms)",
      defaultValue = "5000",
      displayPosition = 26,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP",
      dependsOn = "httpMode",
      triggeredByValue = "POLLING"
  )
  public long pollingInterval = 5000;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      displayPosition = 1,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDef(
          required = false,
          type = ConfigDef.Type.MODEL,
          label = "Per-Status Actions",
          description = "List of actions to take for specific response statuses.",
          displayPosition = 27,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      displayPosition = 28,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  public boolean propagateAllHttpResponses = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Error Response Body Field",
      description = "Field to store the error response body after performing per-status actions",
      defaultValue = "outErrorBody",
      displayPosition = 29,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "propagateAllHttpResponses",
      triggeredByValue = "true",
      group = "HTTP"
  )
  public String errorResponseField = "outErrorBody";

  @ConfigDefBean(groups = "TIMEOUT")
  public HttpTimeoutResponseActionConfigBean responseTimeoutActionConfig =
          new HttpTimeoutResponseActionConfigBean(0, ResponseAction.RETRY_IMMEDIATELY);

  @ConfigDefBean(groups = "PAGINATION")
  public PaginationConfigBean pagination = new PaginationConfigBean();

  /**
   * Validates the parameters for this config bean.
   * @param context Stage Context
   * @param groupName Group name this bean is used in
   * @param prefix Prefix to the parameter names (e.g. parent beans)
   * @param issues List of issues to augment
   */
  public void init(Source.Context context, String groupName, String prefix, List<Stage.ConfigIssue> issues) {

    // Validate the ELs for string configs
    List<String> elConfigs = ImmutableList.of("resourceUrl", "requestBody");
    for (String configName : elConfigs) {
      ELVars vars = context.createELVars();
      vars.addVariable(START_AT, 0);
      ELEval eval = context.createELEval(configName);
      try {
        eval.eval(vars, (String)getClass().getField(configName).get(this), String.class);
      } catch (ELEvalException | NoSuchFieldException | IllegalAccessException e) {
        LOG.error(Errors.HTTP_06.getMessage(), e.toString(), e);
        issues.add(context.createConfigIssue(groupName, prefix + configName, Errors.HTTP_06, e.toString()));
      }
    }

    client.init(context, Groups.PROXY.name(), prefix + "client.", issues);

    // Validate the EL for each header entry
    ELVars headerVars = context.createELVars();
    ELEval headerEval = context.createELEval("headers");
    for (String headerValue : headers.values()) {
      try {
        headerEval.eval(headerVars, headerValue, String.class);
      } catch (ELEvalException e) {
        LOG.error(Errors.HTTP_06.getMessage(), e.toString(), e);
        issues.add(context.createConfigIssue(groupName, prefix + "headers", Errors.HTTP_06, e.toString()));
      }
    }
  }
}
