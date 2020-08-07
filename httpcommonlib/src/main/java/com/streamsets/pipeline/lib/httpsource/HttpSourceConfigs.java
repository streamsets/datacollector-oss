/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.lib.httpsource;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.lib.tls.CredentialValueBean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HttpSourceConfigs extends CommonHttpConfigs{


  @ConfigDefBean(groups = "HTTP")
  public KerberosHttpSourceConfigs spnegoConfigBean = new KerberosHttpSourceConfigs();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "List of Application IDs",
      description = "HTTP requests must contain one of the listed application IDs. " +
          "If none are listed, HTTP requests do not require an application ID.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP"
  )
  @ListBeanModel
  public List<CredentialValueBean> appIds = new ArrayList<>(Arrays.asList(new CredentialValueBean()));


  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Basic Authentication Users",
      description = "User names and passwords for basic authentication. If none specified, basic authentication " +
          "is disabled. It can be used only if the TLS option is enabled.",
      displayPosition = 1380,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TLS",
      dependsOn = "tlsConfigBean.tlsEnabled",
      triggeredByValue = "true"
  )
  @ListBeanModel
  public List<CredentialValueUserPassBean> basicAuthUsers = new ArrayList<>();


  @Override
  public List<CredentialValueBean> getAppIds() {
    return appIds;
  }

  @Override
  public boolean isApplicationIdEnabled(){
    return appIds != null && appIds.size() > 0 && appIds.get(0).get().length() > 0 ;
  }

  public List<CredentialValueUserPassBean> getBasicAuthUsers(){
    return basicAuthUsers;
  }

  public KerberosHttpSourceConfigs getSpnegoConfigBean() {
    return spnegoConfigBean;
  }

}
