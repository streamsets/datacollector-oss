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
package com.streamsets.pipeline.stage.processor.startJob;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;

import java.util.Map;

public class StartJobConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "https://cloud.streamsets.com",
      label = "Control Hub Base URL",
      displayPosition = 10,
      group = "JOB"
  )
  public String baseUrl = "https://cloud.streamsets.com";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Job ID",
      description = "ID of the job to start",
      displayPosition = 20,
      group = "JOB"
  )
  public String jobId = "";

  @ConfigDef(
      required = false,
      defaultValue = "{}",
      type = ConfigDef.Type.MAP,
      label = "Runtime Parameters",
      description = "Runtime parameters to pass to the job",
      displayPosition = 30,
      group = "JOB"
  )
  public Map<String, Object> runtimeParameters;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Reset Origin",
      description = "Reset the origin before starting the job",
      defaultValue = "false",
      displayPosition = 40,
      group = "JOB"
  )
  public boolean resetOrigin = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Run in Background",
      description = "Runs the job in the background, allowing multiple instances of the job to run in parallel. " +
          "When not used, the processor waits until the job is complete before passing the record downstream",
      defaultValue = "false",
      displayPosition = 50,
      group = "JOB"
  )
  public boolean runInBackground = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000",
      label = "Delay Between State Checks",
      description = "Milliseconds to wait before checking pipeline state",
      displayPosition = 60,
      group = "JOB",
      min = 0,
      max = Integer.MAX_VALUE,
      dependsOn = "runInBackground",
      triggeredByValue = { "false" }
  )
  public int waitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "/output",
      label = "Output Field Path",
      description = "Writes the pipeline status, offset, and metrics to the specified field",
      displayPosition = 70,
      group = "JOB"
  )
  public String outputFieldPath = "/output";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Control Hub User Name",
      displayPosition = 71,
      group = "CREDENTIALS"
  )
  public CredentialValue username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      displayPosition = 72,
      group = "CREDENTIALS"
  )
  public CredentialValue password;

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfig = new TlsConfigBean();

}
