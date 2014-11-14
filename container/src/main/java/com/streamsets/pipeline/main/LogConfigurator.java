/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.main;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.net.URL;

public class LogConfigurator {

  private static final String LOG4J_PROPERTIES = "log4j.properties";

  private final RuntimeInfo runtimeInfo;

  @Inject
  public LogConfigurator(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
  }

  public void configure() {
    if (System.getProperty("log4j.configuration") == null) {
      System.setProperty("log4j.defaultInitOverride", "true");
      boolean foundConfig = false;
      boolean fromClasspath = true;
      File log4jConf = new File(runtimeInfo.getConfigDir(), LOG4J_PROPERTIES).getAbsoluteFile();
      if (log4jConf.exists()) {
        PropertyConfigurator.configureAndWatch(log4jConf.getPath(), 1000);
        fromClasspath = false;
        foundConfig = true;
      } else {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL log4jUrl = cl.getResource(LOG4J_PROPERTIES);
        if (log4jUrl != null) {
          PropertyConfigurator.configure(log4jUrl);
          foundConfig = true;
        }
      }
      Logger log = LoggerFactory.getLogger(this.getClass());
      log.debug("Log starting, from configuration: {}", log4jConf.getAbsoluteFile());
      if (!foundConfig) {
        log.warn("Log4j configuration file '{}' not found", LOG4J_PROPERTIES);
      } else if (fromClasspath) {
        log.warn("Log4j configuration file '{}' read from classpath, reload not enabled", LOG4J_PROPERTIES);
      } else {
        log.debug("Log4j configuration file '{}', auto reload enabled", log4jConf.getAbsoluteFile());
      }
    }
  }

}
