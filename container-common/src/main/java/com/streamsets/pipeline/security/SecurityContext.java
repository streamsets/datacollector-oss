/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.security;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.net.InetAddress;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SecurityContext {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityContext.class);

  public final static String KERBEROS_ENABLED_KEY = "kerberos.client.enabled";
  public final static boolean KERBEROS_ENABLED_DEFAULT = false;
  public final static String KERBEROS_PRINCIPAL_KEY = "kerberos.client.principal";
  public final static String KERBEROS_PRINCIPAL_DEFAULT = "sdc/_HOST";
  public final static String KERBEROS_KEYTAB_KEY = "kerberos.client.keytab";
  public final static String KERBEROS_KEYTAB_DEFAULT = "sdc.keytab";

  private final RuntimeInfo runtimeInfo;
  private final boolean kerberosEnabled;
  private final String kerberosPrincipal;
  private final String kerberosKeytab;
  private LoginContext loginContext;
  private Subject subject;

  public SecurityContext(RuntimeInfo runtimeInfo, Configuration serviceConf) {
    this.runtimeInfo = runtimeInfo;
    kerberosEnabled = serviceConf.get(KERBEROS_ENABLED_KEY, KERBEROS_ENABLED_DEFAULT);
    kerberosPrincipal = (kerberosEnabled) ? resolveKerberosPrincipal(serviceConf) : null;
    kerberosKeytab = (kerberosEnabled) ? serviceConf.get(KERBEROS_KEYTAB_KEY, KERBEROS_KEYTAB_DEFAULT) : null;
  }

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

  public String getKerberosPrincipal() {
    return kerberosPrincipal;
  }

  static String getLocalHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public String getKerberosKeytab() {
    return kerberosKeytab;
  }

  public void login() {
    if (subject != null) {
      throw new IllegalStateException(Utils.format("Service already logged-in, Principal '{}'",
                                                   subject.getPrincipals()));
    }
    if (kerberosEnabled) {
      try {
        loginContext = kerberosLogin(true);
        subject = loginContext.getSubject();
      } catch (Exception ex) {
        throw new RuntimeException(Utils.format("It could not get Kerberos credentials: {}", ex.getMessage()), ex);
      }
    } else {
      subject = new Subject();
    }
    LOG.debug("Logged-in. Kerberos enabled '{}', Principal '{}'", kerberosEnabled, subject.getPrincipals());
  }

  public void logout() {
    if (subject != null) {
      LOG.debug("Logout. Kerberos enabled '{}', Principal '{}'", kerberosEnabled, subject.getPrincipals());
      if (loginContext != null) {
        try {
          loginContext.logout();
        } catch (LoginException ex) {
          LOG.warn("Error while doing logout from Kerberos: {}", ex.getMessage(), ex);
        } finally {
          loginContext = null;
        }
      }
      subject = null;
    }
  }

  public Subject getSubject() {
    Utils.checkState(subject != null, "Service is not logged-in");
    return subject;
  }


  private LoginContext kerberosLogin(boolean isClient) throws Exception {
    Subject subject;
    String principalName = getKerberosPrincipal();
    String keytab = getKerberosKeytab();
    if (!(keytab.charAt(0) == '/')) {
      String confDir = runtimeInfo.getConfigDir();
      keytab = new File(confDir, keytab).getAbsolutePath();
    }
    File keytabFile = new File(keytab);
    if (!keytabFile.exists()) {
      throw new RuntimeException(Utils.format("Keytab file '{}' does not exist", keytabFile));
    }
    Set<Principal> principals = new HashSet<>();
    principals.add(new KerberosPrincipal(principalName));
    subject = new Subject(false, principals, new HashSet<>(), new HashSet<>());
    LoginContext context = new LoginContext("", subject, null,
                                            new KeytabKerberosConfiguration(principalName, keytabFile, isClient));
    context.login();
    return context;
  }

  private static String getJvmKrb5LoginModuleName() {
    return System.getProperty("java.vendor").contains("IBM")
           ? "com.ibm.security.auth.module.Krb5LoginModule" : "com.sun.security.auth.module.Krb5LoginModule";
  }

  private static class KeytabKerberosConfiguration extends javax.security.auth.login.Configuration {
    private String principal;
    private String keytab;
    private boolean isInitiator;

    public KeytabKerberosConfiguration(String principal, File keytab, boolean client) {
      this.principal = principal;
      this.keytab = keytab.getAbsolutePath();
      this.isInitiator = client;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<>();
      options.put("keyTab", keytab);
      options.put("principal", principal);
      options.put("useKeyTab", "true");
      options.put("storeKey", "true");
      options.put("doNotPrompt", "true");
      options.put("useTicketCache", "true");
      options.put("renewTGT", "true");
      options.put("refreshKrb5Config", "true");
      options.put("isInitiator", Boolean.toString(isInitiator));
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        options.put("ticketCache", ticketCache);
      }
      options.put("debug", System.getProperty("sun.security.krb5.debug=true", "false"));

      return new AppConfigurationEntry[]{
          new AppConfigurationEntry(getJvmKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                    options)};
    }
  }

}
