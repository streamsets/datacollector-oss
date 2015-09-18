/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * The <code>SecurityContext</code> allows to run a JVM within a client Kerberos session that is propagated to
 * all threads created within a <code>Subject.doAs()</code> invocation using the Subject from the
 * <code></code>
 */
public class SecurityContext {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityContext.class);
  private static final long THIRTY_SECONDS_MS = TimeUnit.SECONDS.toMillis(30);
  private final RuntimeInfo runtimeInfo;
  private final SecurityConfiguration securityConfiguration;
  private final Random random;
  private LoginContext loginContext;
  private Subject subject;
  private Thread renewalThread;
  private double renewalWindow;

  public SecurityContext(RuntimeInfo runtimeInfo, Configuration serviceConf) {
    this.runtimeInfo = runtimeInfo;
    this.securityConfiguration = new SecurityConfiguration(runtimeInfo, serviceConf);
    this.random = new Random();
    // choose a random window between 0.5 and 0.8
    renewalWindow = (random.nextDouble() * 30 + 50)/100;
  }

  public SecurityConfiguration getSecurityConfiguration() {
    return securityConfiguration;
  }

  /**
   * Get the Kerberos TGT
   * @return the user's TGT or null if none was found
   */
  private synchronized KerberosTicket getKerberosTicket() {
    SortedSet<KerberosTicket> tickets = new TreeSet<>(new Comparator<KerberosTicket>() {
      @Override
      public int compare(KerberosTicket ticket1, KerberosTicket ticket2) {
        return Long.compare(ticket1.getEndTime().getTime(), ticket2.getEndTime().getTime());
      }
    });
    for (KerberosTicket ticket : subject.getPrivateCredentials(KerberosTicket.class)) {
      KerberosPrincipal principal = ticket.getServer();
      String principalName = Utils.format("krbtgt/{}@{}", principal.getRealm(), principal.getRealm());
      if (principalName.equals(principal.getName())) {
        tickets.add(ticket);
        calculateRenewalTime(ticket);
      }
    }
    if (tickets.isEmpty()) {
      return null;
    } else {
      return tickets.last(); // most recent ticket
    }
  }

  private synchronized long calculateRenewalTime(KerberosTicket kerberosTicket) {
    long start = kerberosTicket.getStartTime().getTime();
    long end = kerberosTicket.getEndTime().getTime();
    long renewTime = start + (long) ((end - start) * renewalWindow);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Ticket: {}, numPrivateCredentials: {}, ticketStartTime: {}, ticketEndTime: {}, now: {}, renewalTime: {}",
        System.identityHashCode(kerberosTicket), subject.getPrivateCredentials().size(), new Date(start), new Date(end),
        new Date(), new Date(renewTime));
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

  /**
   * Logs in. If Kerberos is enabled it logs in against the KDC, otherwise is a NOP.
   */
  public synchronized void login() {
    if (subject != null) {
      throw new IllegalStateException(Utils.format("Service already logged-in, Principal '{}'",
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
          public void run() {
            while (true) {
              try {
                KerberosTicket kerberosTicket = getKerberosTicket();
                if (kerberosTicket == null) {
                  LOG.error("Could not obtain kerberos ticket");
                  TimeUnit.MILLISECONDS.sleep(THIRTY_SECONDS_MS);
                } else {
                  long renewalTimeMs = calculateRenewalTime(kerberosTicket) - THIRTY_SECONDS_MS;
                  if (renewalTimeMs > 0) {
                    TimeUnit.MILLISECONDS.sleep(renewalTimeMs);
                  }
                  relogin();
                }
              } catch (InterruptedException ie) {
                LOG.info("Interrupted, exiting renewal thread");
                return;
              } catch (Exception exception) {
                LOG.error("Exception in renewal thread: " + exception, exception);
              } catch (Throwable throwable) {
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
    LOG.debug("Logged-in. Kerberos enabled '{}', Principal '{}'", securityConfiguration.isKerberosEnabled(),
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
    LoginContext context = new LoginContext("", subject, null, new KeytabKerberosConfiguration(runtimeInfo,
        principalName, new File(securityConfiguration.getKerberosKeytab()), true));
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
      options.put("debug", System.getProperty("sun.security.krb5.debug", "false"));

      return new AppConfigurationEntry[]{
          new AppConfigurationEntry(getJvmKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                    options)};
    }
  }

}
