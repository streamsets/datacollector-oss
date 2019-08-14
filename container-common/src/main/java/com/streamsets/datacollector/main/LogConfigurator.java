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

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import javax.inject.Inject;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.LogManager;

public class LogConfigurator {
  private final RuntimeInfo runtimeInfo;

  @Inject
  public LogConfigurator(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
  }

  public void configure() {
    if (runtimeInfo.isTransientEnv()
        && System.getProperty("SDC_MESOS_BASE_DIR") == null) {
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

      // Make sure any j.u.l logging is redirected to slf4j logs.
      LogManager.getLogManager().reset();
      SLF4JBridgeHandler.removeHandlersForRootLogger();
      SLF4JBridgeHandler.install();
      // Set j.u.l log level to INFO if root logger has info enabled, else enable WARN.
      Level julLogLevel =
          LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).isInfoEnabled() ? Level.INFO : Level.WARNING;
      java.util.logging.Logger.getLogger("global").setLevel(julLogLevel);
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
