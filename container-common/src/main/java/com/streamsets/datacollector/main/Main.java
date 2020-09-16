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
package com.streamsets.datacollector.main;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.security.SdcSecurityManager;
import com.streamsets.datacollector.security.SecurityContext;
import com.streamsets.datacollector.security.SecurityUtil;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import dagger.ObjectGraph;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import java.lang.management.ManagementFactory;
import java.net.Authenticator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main {
  private final String PROPERTY_USE_SDC_SECURITY_MANAGER = "security_manager.sdc_manager.enable";
  private final boolean DEFAULT_USE_SDC_SECURITY_MANAGER = false;

  private final ObjectGraph dagger;
  private final Task task;
  private final Callable<Boolean> taskStopCondition;

  @VisibleForTesting
  protected Main(Object module, Callable<Boolean> taskStopCondition) {
    this(ObjectGraph.create(module), null, taskStopCondition);
  }

  @VisibleForTesting
  public Main(ObjectGraph dagger, Task task, Callable<Boolean> taskStopCondition) {
    this.dagger = dagger;
    if (task == null) {
      task = dagger.get(TaskWrapper.class);
    }
    this.task = task;
    this.taskStopCondition = taskStopCondition;
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
      RuntimeInfo runtimeInfo = dagger.get(RuntimeInfo.class);
      runtimeInfo.log(log);
      log.info("-----------------------------------------------------------------");
      log.info("Process and Machine Info");
      log.info("  Available Cores: {}", Runtime.getRuntime().availableProcessors());
      log.info("  Total RAM: {}", FileUtils.byteCountToDisplaySize(((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize()));
      log.info("  Non-SDC JVM Args: {}", String.join(" ", ManagementFactory.getRuntimeMXBean().getInputArguments().stream().filter(i -> !i.startsWith("-Dsdc")).collect(Collectors.toList())));
      log.debug("  Full JVM Args: {}", String.join(" ", ManagementFactory.getRuntimeMXBean().getInputArguments()));
      log.info("-----------------------------------------------------------------");
      Configuration configuration = dagger.get(Configuration.class);
      if (System.getSecurityManager() != null) {
        if(configuration.get(PROPERTY_USE_SDC_SECURITY_MANAGER, DEFAULT_USE_SDC_SECURITY_MANAGER)) {
          System.setSecurityManager(new SdcSecurityManager(runtimeInfo, configuration));
        }

        log.info("  Security Manager : ENABLED, policy file: {}, implementation: {}", System.getProperty("java.security.policy"), System.getSecurityManager().getClass().getName());
      } else {
        log.warn("  Security Manager : DISABLED");
      }
      log.info("-----------------------------------------------------------------");
      log.info("Starting ...");

      // Use proxy authenticator that supports username and password
      Authenticator.setDefault(new UserPasswordAuthenticator());

      securityContext = new SecurityContext(dagger.get(RuntimeInfo.class), configuration);
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

      if(configuration.get("monitor.memory", false)) {
        log.warn("Memory monitoring (monitor.memory=true) is no longer supported.");
      }

      log.info("Starting ...");

      final Logger finalLog = log;
      final ShutdownHandler.ShutdownStatus shutdownStatus = new ShutdownHandler.ShutdownStatus();
      final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
      PrivilegedExceptionAction<Void> action = () -> {
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
        if (taskStopCondition != null) {
          //Check every second for the condition to stop the task
          scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
              if (taskStopCondition.call()) {
                task.stop();
              }
            } catch (Exception e) {
              finalLog.error("Error evaluating task stop condition : {}", e);
              throw new RuntimeException(e);
            }
          }, 1,1, TimeUnit.SECONDS);
        }
        task.run();
        task.waitWhileRunning();
        scheduledExecutorService.shutdown();
        try {
          getRuntime().removeShutdownHook(shutdownHookThread);
        } catch (IllegalStateException ignored) {
          // thrown when we try and remove the shutdown
          // hook but it is already running
        }
        finalLog.debug("Stopping, reason: programmatic stop()");
        return null;
      };
      SecurityUtil.doAs(securityContext.getSubject(), action);
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
