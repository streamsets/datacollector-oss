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
package com.streamsets.pipeline.stage.destination.coap;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.stage.destination.http.DataFormatChooserValues;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

/**
 * Bean specifying the configuration for an CoapClientTarget instance.
 */
public class CoapClientTargetConfig {
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
      defaultValue = "coap://localhost:5683/sdc",
      description = "The CoAP resource URL",
      elDefs = RecordEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "COAP"
  )
  public String resourceUrl = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "CoAP Method",
      defaultValue = "POST",
      description = "CoAP method to send",
      elDefs = RecordEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "COAP"
  )
  @ValueChooserModel(CoapMethodChooserValues.class)
  public HttpMethod coapMethod = HttpMethod.POST;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Request Type",
      defaultValue = "NONCONFIRMABLE",
      description = "Specify the type of requests.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "COAP"
  )
  @ValueChooserModel(RequestTypeChooserValues.class)
  public RequestType requestType = RequestType.NONCONFIRMABLE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Connect Timeout",
      defaultValue = "2000",
      description = "CoAP connection timeout in milliseconds. 0 means no timeout.",
      displayPosition = 90,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "COAP"
  )
  public int connectTimeoutMillis = 2000;


}
