/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.main;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

public class LogConfigurator {
  private final RuntimeInfo runtimeInfo;

  @Inject
  public LogConfigurator(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
  }

  public void configure() {
    if (Boolean.getBoolean("sdc.transient-env")) {
      Logger log = LoggerFactory.getLogger(this.getClass());
      log.info("SDC in transient environment, will not reconfigure log");
      return;
    }
    if (System.getProperty("log4j.configuration") == null) {
      URL log4fConfigUrl = null;
      System.setProperty("log4j.defaultInitOverride", "true");
      boolean foundConfig = false;
      boolean fromClasspath = true;
      String log4JProperties = runtimeInfo.getLog4jPropertiesFileName();
      File log4jConf = new File(runtimeInfo.getConfigDir(), log4JProperties).getAbsoluteFile();
      if (log4jConf.exists()) {
        PropertyConfigurator.configureAndWatch(log4jConf.getPath(), 1000);
        fromClasspath = false;
        foundConfig = true;
        try {
          log4fConfigUrl = log4jConf.toURI().toURL();
        } catch (MalformedURLException ex) {
          throw new RuntimeException(ex);
        }
      } else {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL log4jUrl = cl.getResource(log4JProperties);
        if (log4jUrl != null) {
          PropertyConfigurator.configure(log4jUrl);
          foundConfig = true;
          log4fConfigUrl = log4jUrl;
        }
      }
      if (log4fConfigUrl != null) {
        runtimeInfo.setAttribute(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR, log4fConfigUrl);
      }
      Logger log = LoggerFactory.getLogger(this.getClass());
      log.debug("Log starting, from configuration: {}", log4jConf.getAbsoluteFile());
      if (!foundConfig) {
        log.warn("Log4j configuration file '{}' not found", log4JProperties);
      } else if (fromClasspath) {
        log.warn("Log4j configuration file '{}' read from classpath, reload not enabled", log4JProperties);
      } else {
        log.debug("Log4j configuration file '{}', auto reload enabled", log4jConf.getAbsoluteFile());
      }
    }
  }

}
