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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The <code>SecurityContext</code> allows to run a JVM within a client Kerberos session that is propagated to
 * all threads created within a <code>Subject.doAs()</code> invocation using the Subject from the
 * <code></code>
 */
public class SecurityContext {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityContext.class);
  private static final long THIRTY_SECONDS_MS = TimeUnit.SECONDS.toMillis(30);

  private final SecurityConfiguration securityConfiguration;
  private LoginContext loginContext;
  private volatile Subject subject;
  private Thread renewalThread;
  private double renewalWindow;

  public SecurityContext(RuntimeInfo runtimeInfo, Configuration serviceConf) {
    this.securityConfiguration = new SecurityConfiguration(runtimeInfo, serviceConf);
    renewalWindow =  computeRenewalWindow();
  }

  @VisibleForTesting
  double computeRenewalWindow() {
    return (50D + new Random().nextInt(20)) / 100;
  }

  @VisibleForTesting
  double getRenewalWindow() {
    return renewalWindow;
  }

  @VisibleForTesting
  long getRenewalTime(long start, long end) {
    return start + (long) (getRenewalWindow() * (end - start));
  }

  public SecurityConfiguration getSecurityConfiguration() {
    return securityConfiguration;
  }

  @VisibleForTesting
  long getTimeNow() {
    return System.currentTimeMillis();
  }

  @VisibleForTesting
  KerberosTicket getNewestTGT() {
    KerberosTicket tgt = null;
    for (KerberosTicket ticket : getSubject().getPrivateCredentials(KerberosTicket.class)) {
      KerberosPrincipal principal = ticket.getServer();
      String principalName = Utils.format("krbtgt/{}@{}", principal.getRealm(), principal.getRealm());
      if (principalName.equals(principal.getName())) {
        if (LOG.isTraceEnabled()) {
          String ticketString =
              "Found ticket: \n" +
                  "Ticket Server: " + ticket.getServer().getName() + "\n" +
                  "Auth Time: " + ticket.getAuthTime() + "\n" +
                  "Expiry Time: " + ticket.getEndTime();
          LOG.trace(ticketString);
        } else {
          LOG.debug("Found Kerberos ticket '{}'", ticket.getServer().getName());
        }
        if (tgt == null || ticket.getEndTime().after(tgt.getEndTime())) {
          tgt = ticket;
        }
      }
    }
    return tgt;
  }

  private synchronized long calculateRenewalTime(KerberosTicket kerberosTicket) {
    long start = kerberosTicket.getStartTime().getTime();
    long end = kerberosTicket.getEndTime().getTime();
    long renewTime = getRenewalTime(start, end);
    if (LOG.isDebugEnabled()) {
      LOG.trace(
          "Ticket: {}, numPrivateCredentials: {}, ticketStartTime: {}, ticketEndTime: {}, now: {}, renewalTime: {}",
          System.identityHashCode(kerberosTicket),
          getSubject().getPrivateCredentials(KerberosTicket.class).size(),
          new Date(start),
          new Date(end),
          new Date(),
          new Date(renewTime)
      );
    }
    return Math.max(1, renewTime - System.currentTimeMillis());
  }

  private synchronized void relogin() {
    LOG.info("Attempting re-login");
    // do not logout old context, it may be in use
    try {
      loginContext = createLoginContext();
    } catch (Exception ex) {
      throw new RuntimeException(Utils.format("Could not get Kerberos credentials: {}", ex.toString()), ex);
    }
  }

  @VisibleForTesting
  boolean sleep(long millis) {
    try {
      Thread.sleep(millis);
      return true;
    } catch (InterruptedException ex) {
      return false;
    }
  }

  /**
   * Logs in. If Kerberos is enabled it logs in against the KDC, otherwise is a NOP.
   */
  public synchronized void login() {
    if (subject != null) {
      throw new IllegalStateException(Utils.format("Service already login, Principal '{}'",
                                                   subject.getPrincipals()));
    }
    if (securityConfiguration.isKerberosEnabled()) {
      try {
        loginContext = createLoginContext();
        subject = loginContext.getSubject();
      } catch (Exception ex) {
        throw new RuntimeException(Utils.format("Could not get Kerberos credentials: {}", ex.toString()), ex);
      }
      if (renewalThread == null) {
        renewalThread = new Thread() {
          @Override
          public void run() {
            LOG.debug("Starting renewal thread");
            if (!SecurityContext.this.sleep(THIRTY_SECONDS_MS)) {
              LOG.info("Interrupted, exiting renewal thread");
              return;
            }
            while (true) {
              LOG.trace("Renewal check starts");
              try {
                KerberosTicket lastExpiringTGT = getNewestTGT();
                if (lastExpiringTGT == null) {
                  LOG.warn(
                      "Could not obtain kerberos ticket, it may have expired already or it was logged out, will wait" +
                      "30 secs to attempt a relogin"
                  );
                  LOG.trace("Ticket not found, sleeping 30 secs and trying to login");
                  if (!SecurityContext.this.sleep(THIRTY_SECONDS_MS)) {
                    LOG.info("Interrupted, exiting renewal thread");
                    return;
                  }
                } else {
                  long renewalTimeMs = calculateRenewalTime(lastExpiringTGT) - THIRTY_SECONDS_MS;
                  LOG.trace("Ticket found time to renewal '{}ms', sleeping that time", renewalTimeMs);
                  if (renewalTimeMs > 0) {
                    if (!SecurityContext.this.sleep(renewalTimeMs)) {
                      LOG.info("Interrupted, exiting renewal thread");
                      return;
                    }
                  }
                }
                LOG.debug("Triggering relogin");
                Set<KerberosTicket> oldTickets = getSubject().getPrivateCredentials(KerberosTicket.class);
                relogin();
                // Remove all old private credentials, since we only need the new one we just added
                getSubject().getPrivateCredentials().removeAll(oldTickets);
              } catch (Exception exception) {
                LOG.error("Stopping renewal thread because of exception: " + exception, exception);
                return;
              }
              catch (Throwable throwable) {
                LOG.error("Error in renewal thread: " + throwable, throwable);
                return;
              }
            }
          }
        };
        List<String> principals = new ArrayList<>();
        for (Principal p : subject.getPrincipals()) {
          principals.add(p.getName());
        }
        renewalThread.setName("Kerberos-Renewal-Thread-" + Joiner.on(",").join(principals));
        renewalThread.setContextClassLoader(Thread.currentThread().getContextClassLoader());
        renewalThread.setDaemon(true);
        renewalThread.start();
      }
    } else {
      subject = new Subject();
    }
    LOG.debug("Login. Kerberos enabled '{}', Principal '{}'", securityConfiguration.isKerberosEnabled(),
      subject.getPrincipals());
  }

  /**
   * Logs out. If Keberos is enabled it logs out from the KDC, otherwise is a NOP.
   */
  public synchronized void logout() {
    if (subject != null) {
      LOG.debug("Logout. Kerberos enabled '{}', Principal '{}'", securityConfiguration.isKerberosEnabled(),
        subject.getPrincipals());
      if (loginContext != null) {
        try {
          loginContext.logout();
        } catch (LoginException ex) {
          LOG.warn("Error while doing logout from Kerberos: {}", ex.toString(), ex);
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
  public synchronized Subject getSubject() {
    return subject;
  }


  private LoginContext createLoginContext() throws Exception {
    String principalName = securityConfiguration.getKerberosPrincipal();
    if (subject == null) {
      Set<Principal> principals = new HashSet<>();
      principals.add(new KerberosPrincipal(principalName));
      subject = new Subject(false, principals, new HashSet<>(), new HashSet<>());
    }
    LoginContext context = new LoginContext(
        "",
        subject,
        null,
        new KeytabKerberosConfiguration(
            principalName,
            new File(securityConfiguration.getKerberosKeytab()),
            true
        )
    );
    context.login();
    LOG.info("Login, principal '{}'", principalName);
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
      options.put("refreshKrb5Config", "true");
      options.put("isInitiator", Boolean.toString(isInitiator));
      options.put("debug", System.getProperty("sun.security.krb5.debug", "true"));
      // Do not store/use ticket in/from cache. SecurityContext does not renew it by doing something like "kinit -R"
      // Instead a new ticket is requested during the renewal.
      // Credentials are explicitly saved into credential cache ${SDC_DATA}/sdc-krb5.ticketCache for services like
      // Kafka to pick up.
      return new AppConfigurationEntry[]{
          new AppConfigurationEntry(getJvmKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                    options)};
    }
  }

}
