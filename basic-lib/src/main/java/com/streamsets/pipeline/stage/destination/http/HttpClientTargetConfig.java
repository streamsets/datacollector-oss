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
package com.streamsets.pipeline.stage.destination.http;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.el.VaultEL;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.JerseyClientConfigBean;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.pipeline.stage.destination.lib.ToOriginResponseConfig;
import com.streamsets.pipeline.stage.processor.http.HttpMethodChooserValues;

import java.util.HashMap;
import java.util.Map;

/**
 * Bean specifying the configuration for an HttpClientTarget instance.
 */
public class HttpClientTargetConfig {
  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

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
      displayPosition = 60,
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
      defaultValue = "POST",
      description = "HTTP method to send",
      elDefs = RecordEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP"
  )
  @ValueChooserModel(HttpMethodChooserValues.class)
  public HttpMethod httpMethod = HttpMethod.POST;

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

  @ConfigDefBean
  public JerseyClientConfigBean client = new JerseyClientConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "One Request per Batch",
      description = "Generates a single HTTP request with all records in the batch",
      displayPosition = 141,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  public boolean singleRequestPerBatch;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      label = "Rate Limit (Requests/sec)",
      defaultValue = "0",
      description = "Maximum requests per second (0 for unlimited). Useful for rate-limited APIs.",
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
      displayPosition = 250,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  public long maxRequestCompletionSecs = 60L;

  @ConfigDefBean(groups = {"RESPONSE"})
  public ToOriginResponseConfig responseConf = new ToOriginResponseConfig();

}
