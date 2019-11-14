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
package com.streamsets.datacollector.security;

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.File;
import java.net.InetAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SecurityConfiguration {

  public final static String KERBEROS_ENABLED_KEY = "kerberos.client.enabled";
  public final static String KERBEROS_ALWAYS_RESOLVE_PROPS_KEY = "kerberos.client.alwaysResolveProperties";
  public final static boolean KERBEROS_ENABLED_DEFAULT = false;
  public final static boolean KERBEROS_ALWAYS_RESOLVE_PROPS_DEFAULT = false;
  public final static String KERBEROS_PRINCIPAL_KEY = "kerberos.client.principal";
  public final static String KERBEROS_PRINCIPAL_DEFAULT = "sdc/_HOST";
  public final static String KERBEROS_KEYTAB_KEY = "kerberos.client.keytab";
  public final static String KERBEROS_KEYTAB_DEFAULT = "sdc.keytab";

  private final boolean kerberosEnabled;
  private final boolean kerberosAlwaysResolveProperties;
  private final String kerberosPrincipal;
  private final String kerberosKeytab;

  public SecurityConfiguration(RuntimeInfo runtimeInfo, Configuration serviceConf) {
    kerberosEnabled = serviceConf.get(KERBEROS_ENABLED_KEY, KERBEROS_ENABLED_DEFAULT);
    kerberosAlwaysResolveProperties = serviceConf.get(
        KERBEROS_ALWAYS_RESOLVE_PROPS_KEY,
        KERBEROS_ALWAYS_RESOLVE_PROPS_DEFAULT
    );

    final boolean resolveProps = kerberosEnabled || kerberosAlwaysResolveProperties;
    kerberosPrincipal = resolveProps ? resolveKerberosPrincipal(serviceConf) : null;
    if (resolveProps) {
      String keytab = serviceConf.get(KERBEROS_KEYTAB_KEY, KERBEROS_KEYTAB_DEFAULT);
      if (!(keytab.charAt(0) == '/')) {
        String confDir = runtimeInfo.getConfigDir();
        keytab = new File(confDir, keytab).getAbsolutePath();
      }
      File keytabFile = new File(keytab);
      if (!keytabFile.exists()) {
        throw new RuntimeException(Utils.format("Keytab file '{}' does not exist", keytabFile));
      }
      kerberosKeytab = keytab;
    } else {
      kerberosKeytab = null;
    }
  }

  /**
   * Returns if Kerberos authentication is enabled.
   *
   * @return <code>true</code> if Kerberos authentication is enabled, <code>false</code> otherwise.
   */
  public boolean isKerberosEnabled() {
    return kerberosEnabled;
  }

  private String resolveKerberosPrincipal(Configuration conf) {
    String principal = conf.get(KERBEROS_PRINCIPAL_KEY, KERBEROS_PRINCIPAL_DEFAULT);
    int idx = -1;
    int len = -1;
    Matcher matcher = Pattern.compile("\\W_HOST(\\W|$)").matcher(principal);
    if (matcher.find()) {
      idx = matcher.start() + 1;
      len = "_HOST".length();
    } else {
      matcher = Pattern.compile("\\W0.0.0.0(\\W|$)").matcher(principal);
      if (matcher.find()) {
        idx = matcher.start() + 1;
        len = "0.0.0.0".length();
      }
    }
    if (idx > -1) {
      principal = principal.substring(0, idx) + getLocalHostName() + principal.substring(idx + len);
    }
    return principal;
  }

  /**
   * Returns the Kerberos principal being used.
   *
   * @return the Kerberos principal being used, or <code>null</code> if Kerberos authentication is not enabled.
   */
  public String getKerberosPrincipal() {
    return kerberosPrincipal;
  }

  /**
   * Returns the Kerberos keytab being used.
   *
   * @return the Kerberos keytab being used, or <code>null</code> if Kerberos authentication is not enabled.
   */

  public String getKerberosKeytab() {
    return kerberosKeytab;
  }

  static String getLocalHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
