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
package com.streamsets.pipeline.lib.startPipeline;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;

import java.util.List;

public class StartPipelineConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Task Name",
      description = "Task name to use in the generated record. Must be unique within the pipeline.",
      displayPosition = 5,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PIPELINE"
  )
  public String taskName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "http://localhost:18630",
      label = "Execution Engine URL",
      description = "URL of Data Collector, Edge, or Transformer to run the pipelines",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PIPELINE"
  )
  public String baseUrl = "http://localhost:18630";

  @ConfigDef(
      label = "Pipelines",
      required = true,
      type = ConfigDef.Type.MODEL,
      description="Pipelines to start in parallel",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PIPELINE"
  )
  @ListBeanModel
  public List<PipelineIdConfig> pipelineIdConfigList;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Reset Origin",
      description = "When possible, resets the origin before starting a pipeline",
      defaultValue = "false",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PIPELINE"
  )
  public boolean resetOrigin = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Control Hub Enabled",
      description = "Indicates the execution engine is registered with Control Hub",
      defaultValue = "false",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PIPELINE"
  )
  public boolean controlHubEnabled = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "https://cloud.streamsets.com",
      label = "Control Hub URL",
      description = "Control Hub URL",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PIPELINE",
      dependsOn = "controlHubEnabled",
      triggeredByValue = { "true" }
  )
  public String controlHubUrl = "https://cloud.streamsets.com";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Run in Background",
      description = "Run started pipelines in the background and pass the record to the next stage immediately. " +
          "If not selected, the record passes downstream after all started pipelines finish.",
      defaultValue = "false",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PIPELINE"
  )
  public boolean runInBackground = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000",
      label = "Status Check Interval",
      description = "Milliseconds to wait between pipeline status checks",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "PIPELINE",
      min = 0,
      max = Integer.MAX_VALUE,
      dependsOn = "runInBackground",
      triggeredByValue = { "false" }
  )
  public int waitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "User Name",
      description = "User to run the pipeline. Enter a Data Collector, Edge, or Transformer user, " +
          "or specify a Control Hub user for execution engines registered with Control Hub",
      defaultValue = "admin",
      displayPosition = 81,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CREDENTIALS"
  )
  public CredentialValue username = () -> "admin";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "Password",
      defaultValue = "admin",
      displayPosition = 82,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CREDENTIALS"
  )
  public CredentialValue password = () -> "admin";

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfig = new TlsConfigBean();

}
