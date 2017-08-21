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
package com.streamsets.datacollector.pipeline.executor.spark;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.el.VaultEL;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

public class CredentialsConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Username",
      displayPosition = 10,
      elDefs = VaultEL.class,
      group = "CREDENTIALS",
      dependsOn = "clusterManager^",
      triggeredByValue = "DATABRICKS"
  )
  public String username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Password",
      displayPosition = 20,
      elDefs = VaultEL.class,
      group = "CREDENTIALS",
      dependsOn = "clusterManager^",
      triggeredByValue = "DATABRICKS"
  )
  public String password;

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      required = false,
      label = "Kerberos Principal",
      group = "CREDENTIALS",
      dependsOn = "clusterManager^",
      triggeredByValue = "YARN"
  )
  public String principal = "";

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      required = false,
      label = "Kerberos Keytab",
      group = "CREDENTIALS",
      dependsOn = "clusterManager^",
      triggeredByValue = "YARN"
  )
  public String keytab = "";

  public HttpAuthenticationFeature init() {
    if (!StringUtils.isEmpty(username)) {
      return HttpAuthenticationFeature.basic(username, password);
    } else {
      return null;
    }
  }
}
