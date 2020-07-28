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


public class KerberosHttpSourceConfigs {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Kerberos with SPNEGO Authentication",
      description = "Use this stage as a service for Kerberos with SPEGNO authentication. Requires access to the krb5.conf file and the login.conf file.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  public boolean spnegoEnabled = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Kerberos Realm",
      description = "Kerberos realm where the service is located",
      displayPosition = 41,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP",
      dependsOn = "spnegoEnabled",
      triggeredByValue = "true"
  )
  public String kerberosRealm = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "HTTP-SPNEGO Principal",
      description = "HTTP service principal declared on the KDC.",
      displayPosition = 42,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP",
      dependsOn = "spnegoEnabled",
      triggeredByValue = "true"
  )
  public String spnegoPrincipal = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Keytab file",
      description = "Path where to find the keytab that contains the entry of the HTTP-SPNEGO Principal.",
      displayPosition = 42,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP",
      dependsOn = "spnegoEnabled",
      triggeredByValue = "true"
  )
  public String spnegoKeytabFilePath = "";

  public boolean isSpnegoEnabled(){
    return spnegoEnabled;
  }

  public String getKerberosRealm(){
    return kerberosRealm;
  }

  public String getSpnegoPrincipal() {
    return spnegoPrincipal;
  }

  public String getSpnegoKeytabFilePath() {
    return spnegoKeytabFilePath;
  }
}