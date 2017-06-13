/**
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
package com.streamsets.datacollector.pipeline.executor.spark.databricks;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.el.VaultEL;
import org.apache.commons.lang3.StringUtils;

public class HttpProxyConfigBean {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Proxy URI",
      dependsOn = "clusterManager^^",
      triggeredByValue = "DATABRICKS",
      group = "PROXY"
  )
  public String uri = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Username",
      dependsOn = "clusterManager^^",
      triggeredByValue = "DATABRICKS",
      elDefs = VaultEL.class,
      group = "PROXY"
  )
  public String username = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Password",
      dependsOn = "clusterManager^^",
      triggeredByValue = "DATABRICKS",
      elDefs = VaultEL.class,
      group = "PROXY"
  )
  public String password = ""; // NOSONAR

  private com.streamsets.pipeline.lib.http.HttpProxyConfigBean underlying;

  public void init() {
    underlying = new com.streamsets.pipeline.lib.http.HttpProxyConfigBean();
    if (!StringUtils.isEmpty(uri)) {
      underlying.uri = uri;
      underlying.username = username;
      underlying.password = password;
    }
  }

  public com.streamsets.pipeline.lib.http.HttpProxyConfigBean getUnderlyingConfig() {
    return underlying;
  }
}
