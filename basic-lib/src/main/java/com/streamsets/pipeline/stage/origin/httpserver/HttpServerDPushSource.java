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
package com.streamsets.pipeline.stage.origin.httpserver;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.DataFormatChooserValues;
import com.streamsets.pipeline.lib.httpsource.HttpSourceConfigs;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import static com.streamsets.pipeline.config.OriginAvroSchemaSource.SOURCE;

@StageDef(
    version = 15,
    label = "HTTP Server",
    description = "Listens for requests on an HTTP endpoint",
    icon="httpserver_multithreaded.png",
    execution = {ExecutionMode.STANDALONE, ExecutionMode.EDGE},
    recordsByRef = true,
    onlineHelpRefUrl ="index.html?contextID=task_pgw_b3b_4y",
    upgrader = HttpServerPushSourceUpgrader.class,
    upgraderDef = "upgrader/HttpServerDPushSource.yaml"
)
@ConfigGroups(Groups.class)
@HideConfigs(value = {
    "dataFormatConfig.verifyChecksum",
    "dataFormatConfig.avroSchemaSource",
    "httpConfigs.tlsConfigBean.useRemoteTrustStore",
    "httpConfigs.tlsConfigBean.trustStoreFilePath",
    "httpConfigs.tlsConfigBean.trustedCertificates",
    "httpConfigs.tlsConfigBean.trustStoreType",
    "httpConfigs.tlsConfigBean.trustStorePassword",
    "httpConfigs.tlsConfigBean.trustStoreAlgorithm",
    "httpConfigs.needClientAuth",
    "httpConfigs.useApiGateway",
    "httpConfigs.serviceName",
    "httpConfigs.needGatewayAuth"
})
@GenerateResourceBundle
public class HttpServerDPushSource extends DPushSource {

  @ConfigDefBean
  public HttpSourceConfigs httpConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Request Size (MB)",
      defaultValue = "100",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxRequestSizeMB;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      description = "HTTP payload data format",
      displayPosition = 1,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = "DATA_FORMAT")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @Override
  protected PushSource createPushSource() {
    if (dataFormat == DataFormat.AVRO) {
      dataFormatConfig.avroSchemaSource = SOURCE;
    }
    return new HttpServerPushSource(httpConfigs, maxRequestSizeMB, dataFormat, dataFormatConfig);
  }

}
