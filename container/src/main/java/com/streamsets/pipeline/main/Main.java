/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.main;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.task.Task;
import com.streamsets.pipeline.task.TaskWrapper;
import dagger.ObjectGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Main {
  private final Class moduleClass;

  public Main() {
    this(PipelineTaskModule.class);
  }

  @VisibleForTesting
  Main(Class moduleClass) {
    this.moduleClass = moduleClass;
  }

  @VisibleForTesting
  Runtime getRuntime() {
    return Runtime.getRuntime();
  }

  public int doMain() {
    Logger log = null;
    try {
      ObjectGraph dagger = ObjectGraph.create(moduleClass);
      final Task task = dagger.get(TaskWrapper.class);

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

      task.init();
      final Logger finalLog = log;
      Thread shutdownHookThread = new Thread("Main.shutdownHook") {
        @Override
        public void run() {
          finalLog.debug("Stopping, reason: SIGTERM (kill)");
          task.stop();
        }
      };
      getRuntime().addShutdownHook(shutdownHookThread);
      dagger.get(RuntimeInfo.class).setShutdownHandler(new Runnable() {
        @Override
        public void run() {
          finalLog.debug("Stopping, reason: requested");
          task.stop();
        }
      });
      task.run();
      task.waitWhileRunning();
      getRuntime().removeShutdownHook(shutdownHookThread);
      log.debug("Stopping, reason: programmatic stop()");
      return 0;
    } catch (Throwable ex) {
      if (log != null) {
        log.error("Abnormal exit: {}", ex.getMessage(), ex);
      }
      System.out.println();
      System.out.printf(Utils.format("Abnormal exit: {}", ex.getMessage()));
      System.out.printf("Check STDERR for more details");
      System.out.println();
      System.err.println();
      ex.printStackTrace(System.err);
      System.err.println();
      return 1;
    }
  }

  public static void setClassLoaders(ClassLoader apiCL, ClassLoader containerCL,
      List<? extends ClassLoader> moduleCLs) {
    RuntimeModule.setStageLibraryClassLoaders(moduleCLs);
  }

  public static void main(String[] args) throws Exception {
    System.exit(new Main().doMain());
  }

}
