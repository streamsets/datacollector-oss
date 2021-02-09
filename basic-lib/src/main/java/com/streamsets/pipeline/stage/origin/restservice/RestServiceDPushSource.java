/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.restservice;

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
import com.streamsets.pipeline.lib.microservice.ResponseConfigBean;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import static com.streamsets.pipeline.config.OriginAvroSchemaSource.SOURCE;

@StageDef(
    version = 7,
    label = "REST Service",
    description = "Listens for requests on an HTTP endpoint and sends response back",
    icon="api.png",
    execution = {ExecutionMode.STANDALONE},
    recordsByRef = true,
    sendsResponse = true,
    onlineHelpRefUrl ="index.html?contextID=task_upp_lgp_q2b",
    upgrader = RestServicePushSourceUpgrader.class,
    upgraderDef = "upgrader/RestServiceDPushSource.yaml"
)
@ConfigGroups(Groups.class)
@HideConfigs(value = {
    "dataFormatConfig.verifyChecksum",
    "dataFormatConfig.avroSchemaSource"
})
@GenerateResourceBundle
public class RestServiceDPushSource extends DPushSource {

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
      defaultValue = "JSON",
      displayPosition = 1,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDefBean(groups = "DATA_FORMAT")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDefBean(groups = "HTTP_RESPONSE")
  public ResponseConfigBean responseConfig = new ResponseConfigBean();

  @Override
  protected PushSource createPushSource() {
    if (dataFormat == DataFormat.AVRO) {
      dataFormatConfig.avroSchemaSource = SOURCE;
    }
    return new RestServicePushSource(
        httpConfigs,
        maxRequestSizeMB,
        dataFormat,
        dataFormatConfig,
        responseConfig
    );
  }

}
