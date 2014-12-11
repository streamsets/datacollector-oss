/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.main;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;

import java.util.List;

public class RuntimeInfo {
  private final List<? extends ClassLoader> stageLibraryClassLoaders;
  private Runnable shutdownRunnable;

  public RuntimeInfo(List<? extends ClassLoader> stageLibraryClassLoaders) {
    this.stageLibraryClassLoaders = ImmutableList.copyOf(stageLibraryClassLoaders);
  }

  public String getRuntimeDir() {
    return System.getProperty("user.dir");
  }

  public String getStaticWebDir() {
    return System.getProperty("pipeline.static-web.dir", getRuntimeDir() + "/static-web");
  }

  public String getConfigDir() {
    return System.getProperty("pipeline.conf.dir", getRuntimeDir() + "/etc");
  }

  public String getLogDir() {
    return System.getProperty("pipeline.log.dir", getRuntimeDir() + "/log");
  }

  public String getDataDir() {
    return System.getProperty("pipeline.data.dir", getRuntimeDir() + "/var");
  }

  public List<? extends ClassLoader> getStageLibraryClassLoaders() {
    return stageLibraryClassLoaders;
  }

  public void log(Logger log) {
    log.info("Runtime info:");
    log.info("  Java version : {}", System.getProperty("java.runtime.version"));
    log.info("  Runtime dir  : {}", getRuntimeDir());
    log.info("  Config dir   : {}", getConfigDir());
    log.info("  Data dir     : {}", getDataDir());
    log.info("  Log dir      : {}", getLogDir());
  }

  public void setShutdownHandler(Runnable runnable) {
    shutdownRunnable = runnable;
  }

  public void shutdown() {
    if (shutdownRunnable != null) {
      shutdownRunnable.run();
    }
  }
}
