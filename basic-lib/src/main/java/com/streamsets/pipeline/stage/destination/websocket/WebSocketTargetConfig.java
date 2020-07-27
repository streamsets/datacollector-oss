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
package com.streamsets.pipeline.stage.destination.websocket;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import com.streamsets.pipeline.stage.destination.http.DataFormatChooserValues;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.Collections;
import java.util.List;

/**
 * Bean specifying the configuration for an WebSocket Client Target instance.
 */
public class WebSocketTargetConfig {
  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      description = "Data Format of the response.",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Resource URL",
      defaultValue = "ws://localhost:8080",
      description = "The WebSocket resource URL",
      elDefs = RecordEL.class,
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "WEB_SOCKET"
  )
  public String resourceUrl = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Headers",
      description = "Headers to include in the request",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "WEB_SOCKET"
  )
  @ListBeanModel
  public List<HeaderBean> headers = Collections.emptyList();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Request Time (sec)",
      defaultValue = "60",
      description = "Maximum time to wait for each request completion.",
      displayPosition = 999,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "WEB_SOCKET"
  )
  public long maxRequestCompletionSecs = 60L;

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfig = new TlsConfigBean();
}
