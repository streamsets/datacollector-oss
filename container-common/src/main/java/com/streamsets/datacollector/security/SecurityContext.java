/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.security;

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;

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

/**
 * The <code>SecurityContext</code> allows to run a JVM within a client Kerberos session that is propagated to
 * all threads created within a <code>Subject.doAs()</code> invocation using the Subject from the
 * <code></code>
 */
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
    if (kerberosEnabled) {
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

  static String getLocalHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Returns the Kerberos keytab absolute path.
   *
   * @return the Kerberos keytab absolute path, or <code>null</code> if Kerberos authentication is not enabled.
   */
  public String getKerberosKeytab() {
    return kerberosKeytab;
  }

  /**
   * Logs in. If Kerberos is enabled it logs in against the KDC, otherwise is a NOP.
   */
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

  /**
   * Logs out. If Keberos is enabled it logs out from the KDC, otherwise is a NOP.
   */
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

  /**
   * Returns the current <code>Subject</code> after a login.
   * <p/>
   * If Kerberos is enabled it returns a <code>Subject</code> with Kerberos credentials.
   * <p/>
   * If Kerberos is not enabled it returns a default <code>Subject</code>.
   *
   * @return the login <code>Subject</code>, or <code>null</code> if not logged in.
   */
  public Subject getSubject() {
    return subject;
  }


  private LoginContext kerberosLogin(boolean isClient) throws Exception {
    Subject subject;
    String principalName = getKerberosPrincipal();
    Set<Principal> principals = new HashSet<>();
    principals.add(new KerberosPrincipal(principalName));
    subject = new Subject(false, principals, new HashSet<>(), new HashSet<>());
    LoginContext context = new LoginContext("", subject, null, new KeytabKerberosConfiguration(runtimeInfo,
        principalName, new File(getKerberosKeytab()), isClient));
    context.login();
    return context;
  }

  private static String getJvmKrb5LoginModuleName() {
    return System.getProperty("java.vendor").contains("IBM")
           ? "com.ibm.security.auth.module.Krb5LoginModule" : "com.sun.security.auth.module.Krb5LoginModule";
  }

  private static class KeytabKerberosConfiguration extends javax.security.auth.login.Configuration {
    private RuntimeInfo runtimeInfo;
    private String principal;
    private String keytab;
    private boolean isInitiator;

    public KeytabKerberosConfiguration(RuntimeInfo runtimeInfo, String principal, File keytab, boolean client) {
      this.runtimeInfo = runtimeInfo;
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
      } else {
        options.put("ticketCache", new File(runtimeInfo.getDataDir(), "krb5.ticketCache").getAbsolutePath());
      }
      options.put("debug", System.getProperty("sun.security.krb5.debug=true", "false"));

      return new AppConfigurationEntry[]{
          new AppConfigurationEntry(getJvmKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                    options)};
    }
  }

}
