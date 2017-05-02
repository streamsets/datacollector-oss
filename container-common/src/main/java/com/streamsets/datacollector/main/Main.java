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
package com.streamsets.datacollector.main;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.security.SecurityContext;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import dagger.ObjectGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.security.auth.Subject;
import java.net.Authenticator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;

public class Main {
  private final ObjectGraph dagger;
  private final Task task;

  @VisibleForTesting
  protected Main(Class moduleClass) {
    this(ObjectGraph.create(moduleClass), null);
  }
  @VisibleForTesting
  public Main(ObjectGraph dagger, Task task) {
    this.dagger = dagger;
    if (task == null) {
      task = dagger.get(TaskWrapper.class);
    }
    this.task = task;
  }

  @VisibleForTesting
  Runtime getRuntime() {
    return Runtime.getRuntime();
  }

  public int doMain() {
    SecurityContext securityContext;
    Logger log = null;
    try {
      final Task task = this.task;
      dagger.get(LogConfigurator.class).configure();
      log = LoggerFactory.getLogger(Main.class);
      log.info("-----------------------------------------------------------------");
      dagger.get(BuildInfo.class).log(log);
      log.info("-----------------------------------------------------------------");
      dagger.get(RuntimeInfo.class).log(log);
      log.info("-----------------------------------------------------------------");
      if (System.getSecurityManager() != null) {
        log.info("  Security Manager : ENABLED, policy file: {}", System.getProperty("java.security.policy"));
      } else {
        log.warn("  Security Manager : DISABLED");
      }
      log.info("-----------------------------------------------------------------");
      log.info("Starting ...");

      // Use proxy authenticator that supports username and password
      Authenticator.setDefault(new UserPasswordAuthenticator());

      securityContext = new SecurityContext(dagger.get(RuntimeInfo.class), dagger.get(Configuration.class));
      securityContext.login();

      log.info("-----------------------------------------------------------------");
      log.info("  Kerberos enabled: {}", securityContext.getSecurityConfiguration().isKerberosEnabled());
      if (securityContext.getSecurityConfiguration().isKerberosEnabled()) {
        log.info("  Kerberos principal: {}", securityContext.getSecurityConfiguration().getKerberosPrincipal());
        log.info("  Kerberos keytab: {}", securityContext.getSecurityConfiguration().getKerberosKeytab());
      }
      try {
        boolean unlimited = Cipher.getMaxAllowedKeyLength("RC5") >= 256;
        log.info("  Unlimited cryptography enabled: {}", unlimited);
      } catch(NoSuchAlgorithmException ex) {
        log.info("  Unlimited cryptography check: algorithm RC5 not found." );
      }
      log.info("-----------------------------------------------------------------");
      log.info("Starting ...");

      final Logger finalLog = log;
      final ShutdownHandler.ShutdownStatus shutdownStatus = new ShutdownHandler.ShutdownStatus();
      Subject.doAs(securityContext.getSubject(), new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          task.init();
          Thread shutdownHookThread = new Thread("Main.shutdownHook") {
            @Override
            public void run() {
              finalLog.debug("Stopping, reason: SIGTERM (kill)");
              task.stop();
            }
          };
          getRuntime().addShutdownHook(shutdownHookThread);
          dagger.get(RuntimeInfo.class).setShutdownHandler(new ShutdownHandler(finalLog, task, shutdownStatus));
          task.run();
          task.waitWhileRunning();
          try {
            getRuntime().removeShutdownHook(shutdownHookThread);
          } catch (IllegalStateException ignored) {
            // thrown when we try and remove the shutdown
            // hook but it is already running
          }
          finalLog.debug("Stopping, reason: programmatic stop()");
          return null;
        }
      });
      return shutdownStatus.getExitStatus();
    } catch (Throwable ex) {
      if (log != null) {
        log.error("Abnormal exit: {}", ex.toString(), ex);
      }
      System.out.println();
      System.out.printf(Utils.format("Abnormal exit: {}", ex.toString()));
      System.out.printf("Check STDERR for more details");
      System.out.println();
      System.err.println();
      ex.printStackTrace(System.err);
      System.err.println();
      return 1;
    }
  }

}
