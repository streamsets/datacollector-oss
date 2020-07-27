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
package com.streamsets.pipeline.stage.destination.splunk;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.el.RecordEL;

/**
 * Bean specifying the configuration for an HttpClientTarget instance.
 */
public class SplunkTargetConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Splunk API Endpoint",
      description = "Splunk API Endpoint, in the form https://server.example.com:8089/",
      elDefs = RecordEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SPLUNK"
  )
  public String splunkEndpoint = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Splunk Token",
      description = "Splunk HTTP Event Collector token",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SPLUNK"
  )
  public CredentialValue splunkToken = () -> "";

  @ConfigDefBean
  public SplunkJerseyClientConfigBean client = new SplunkJerseyClientConfigBean();
}
