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
package com.streamsets.pipeline.stage.origin.httpserver.nifi;

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
import com.streamsets.pipeline.lib.httpsource.RawHttpConfigs;
import com.streamsets.pipeline.stage.origin.httpserver.Groups;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.pipeline.stage.origin.lib.FlowFileDataFormatChooserValues;
import com.streamsets.pipeline.stage.origin.lib.FlowFileVersion;
import com.streamsets.pipeline.stage.origin.lib.FlowFileVersionsChooserValues;
import com.streamsets.pipeline.stage.origin.lib.OuterDataParserFormatConfig;

@StageDef(
    version = 4,
    label = "NiFi HTTP Server",
    description = "Listens for requests from a NiFi HTTP endpoint",
    icon="Apache-nifi-logo.png",
    execution = {ExecutionMode.STANDALONE},
    recordsByRef = true,
    upgraderDef = "upgrader/NiFiHttpServerDPushSource.yaml",
    onlineHelpRefUrl ="" //TODO
)
@ConfigGroups(Groups.class)
@HideConfigs(value = {
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
public class NiFiHttpServerDPushSource extends DPushSource {

  @ConfigDefBean
  public RawHttpConfigs httpConfigs;

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
      label = "FlowFile Version",
      description = "Version of FlowFile",
      defaultValue = "FLOWFILE_V3",
      displayPosition = 1,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(FlowFileVersionsChooserValues.class)
  public FlowFileVersion flowFileVersion = FlowFileVersion.FLOWFILE_V3;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(FlowFileDataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = "DATA_FORMAT")
  public DataParserFormatConfig dataFormatConfig;

  @Override
  protected PushSource createPushSource() {
    OuterDataParserFormatConfig outerConfig = new OuterDataParserFormatConfig(
        DataFormat.FLOWFILE,
        dataFormat,
        dataFormatConfig,
        flowFileVersion
    );
    return new HttpServerPushSourceNiFi(httpConfigs, maxRequestSizeMB, DataFormat.FLOWFILE, outerConfig);
  }

}
