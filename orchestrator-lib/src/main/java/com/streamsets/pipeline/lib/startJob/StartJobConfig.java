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
package com.streamsets.pipeline.lib.startJob;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;

import java.util.List;

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
      type = ConfigDef.Type.BOOLEAN,
      label = "Job Template",
      defaultValue = "false",
      displayPosition = 11,
      group = "JOB"
  )
  public boolean jobTemplate = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Job Template ID",
      description = "ID of the job template to start",
      displayPosition = 12,
      group = "JOB",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      dependsOn = "jobTemplate",
      triggeredByValue = { "true" }
  )
  public String templateJobId = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Instance Name Suffix",
      defaultValue = "COUNTER",
      description = "Job Instances name suffix",
      displayPosition = 13,
      group = "JOB",
      dependsOn = "jobTemplate",
      triggeredByValue = { "true" }
  )
  @ValueChooserModel(InstanceNameSuffixChooserValues.class)
  public InstanceNameSuffix instanceNameSuffix = InstanceNameSuffix.COUNTER;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Parameter Name",
      description = "Name of the parameter",
      displayPosition = 14,
      group = "JOB",
      dependsOn = "instanceNameSuffix",
      triggeredByValue = { "PARAM_VALUE" }
  )
  public String parameterName = "";

  @ConfigDef(
      required = false,
      defaultValue = "[{}]",
      type = ConfigDef.Type.TEXT,
      label = "Runtime Parameters for Each Instance",
      displayPosition = 30,
      group = "JOB",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      dependsOn = "jobTemplate",
      triggeredByValue = { "true" }
  )
  public String runtimeParametersList;

  @ConfigDef(
      label = "Jobs",
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      description="Jobs to start in parallel",
      displayPosition = 20,
      group = "JOB",
      dependsOn = "jobTemplate",
      triggeredByValue = { "false" }
  )
  @ListBeanModel
  public List<JobIdConfig> jobIdConfigList;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Reset Origin",
      description = "Reset the origin before starting the job",
      defaultValue = "false",
      displayPosition = 40,
      group = "JOB",
      dependsOn = "jobTemplate",
      triggeredByValue = { "false" }
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
