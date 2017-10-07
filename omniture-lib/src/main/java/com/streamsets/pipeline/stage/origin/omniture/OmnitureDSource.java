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
package com.streamsets.pipeline.stage.origin.omniture;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.configurablestage.DSource;

@StageDef(
    version = 2,
    label = "Omniture",
    description = "Retrieves Omniture reports via the REST API.",
    icon="omniture_icon.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    upgrader = OmnitureSourceUpgrader.class,
    onlineHelpRefUrl = "index.html#Origins/Omniture.html#task_of4_wpw_1s"
)

@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class OmnitureDSource extends DSource {

  @ConfigDefBean(groups = "PROXY")
  public HttpProxyConfigBean proxySettings = new HttpProxyConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Omniture REST URL",
      defaultValue = "https://api2.omniture.com/admin/1.4/rest/",
      description = "Specify the Omniture REST endpoint",
      displayPosition = 10,
      group = "OMNITURE"
  )
  public String resourceUrl;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      label = "Omniture Report Description",
      description = "Report description to queue",
      displayPosition = 15,
      mode = ConfigDef.Mode.JSON,
      dependsOn = "httpMode",
      lines = 5,
      triggeredByValue = "POLLING",
      group = "REPORT"
  )
  public String reportDescription;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Request Timeout",
      defaultValue = "3000",
      description = "HTTP request timeout in milliseconds.",
      displayPosition = 20,
      group = "OMNITURE"
  )
  public long requestTimeoutMillis;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Mode",
      defaultValue = "POLLING",
      displayPosition = 25,
      group = "OMNITURE"
  )
  @ValueChooserModel(HttpClientModeChooserValues.class)
  public HttpClientMode httpMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Report Request Interval (ms)",
      defaultValue = "5000",
      displayPosition = 30,
      group = "OMNITURE",
      dependsOn = "httpMode",
      triggeredByValue = "POLLING"
  )
  public long pollingInterval;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Batch Size (reports)",
      defaultValue = "1",
      description = "Maximum number of response entities to queue (e.g. JSON objects).",
      displayPosition = 35,
      group = "OMNITURE"
  )
  public int batchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Batch Wait Time (ms)",
      defaultValue = "5000",
      description = "Maximum amount of time to wait to fill a batch before sending it",
      displayPosition = 40,
      group = "OMNITURE"
  )
  public long maxBatchWaitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      description = "Omniture Username",
      displayPosition = 45,
      group = "OMNITURE"
  )
  public CredentialValue username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Shared Secret",
      description = "Omniture Shared Secret",
      displayPosition = 50,
      group = "OMNITURE"
  )
  public CredentialValue sharedSecret;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Proxy",
      description = "Whether or not HTTP proxy should be used for connection",
      defaultValue = "false",
      displayPosition = 60,
      group = "OMNITURE"
  )
  public boolean useProxy;

  @Override
  protected Source createSource() {
    OmnitureConfig config = new OmnitureConfig();
    config.setPollingInterval(pollingInterval);
    config.setMaxBatchWaitTime(maxBatchWaitTime);
    config.setBatchSize(batchSize);
    config.setHttpMode(httpMode);
    config.setReportDescription(reportDescription);
    config.setRequestTimeoutMillis(requestTimeoutMillis);
    config.setResourceUrl(resourceUrl);
    config.setSharedSecret(sharedSecret);
    config.setUsername(username);
    if (useProxy) {
      config.setProxySettings(proxySettings);
    }

    return new OmnitureSource(config);
  }
}
