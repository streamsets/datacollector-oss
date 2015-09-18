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
package com.streamsets.datacollector.log;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class LogUtils {
  public static final String LOG4J_FILE_ATTR = "log4j.filename";
  public static final String LOG4J_APPENDER_STREAMSETS_FILE_PROPERTY = "log4j.appender.streamsets.File";

  public static String getLogFile(RuntimeInfo runtimeInfo) throws IOException {
    if(Boolean.getBoolean("sdc.transient-env")) {
      // we are running under YARN
      String logDirs = System.getenv("LOG_DIRS");
      if (logDirs == null) {
        if(Boolean.getBoolean("sdc.testing-mode")) {
          logDirs = System.getProperty("user.dir") + "/target/";
        } else {
          throw new IllegalStateException("When running in transient environment, environment variable " +
            "LOG_DIRS must be defined");
        }
      }
      File syslog = new File(logDirs, "syslog");
      if (syslog.isFile()) {
        return syslog.getAbsolutePath();
      }
      // fall back to stderr
      return (new File(logDirs, "stderr")).getAbsolutePath();
    }
    String logFile = runtimeInfo.getAttribute(LOG4J_FILE_ATTR);
    if (logFile == null) {
      URL log4jConfig = runtimeInfo.getAttribute(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR);
      if (log4jConfig != null) {
        try (InputStream is = log4jConfig.openStream()) {
          Properties props = new Properties();
          props.load(is);
          logFile = props.getProperty(LOG4J_APPENDER_STREAMSETS_FILE_PROPERTY);
          if (logFile != null) {
            logFile = resolveValue(logFile);
          } else {
            throw new IOException(Utils.format("Could not determine the log file, '{}' does not define property '{}'",
                                               RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR,
                                               LOG4J_APPENDER_STREAMSETS_FILE_PROPERTY));
          }
          if (!logFile.endsWith(".log")) {
            throw new IOException(Utils.format("Log file '{}' must end with '.log',", logFile));
          }
          runtimeInfo.setAttribute(LOG4J_FILE_ATTR, logFile);
        }
      } else {
        throw new IOException(Utils.format("RuntimeInfo does not has attribute '{}'",
                                           RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR));
      }
    }
    return logFile;
  }

  @VisibleForTesting
  static String resolveValue(String str) {
    while (str.contains("${")) {
      int start = str.indexOf("${");
      int end = str.indexOf("}", start);
      String value = System.getProperty(str.substring(start + 2, end));
      String current = str;
      str = current.substring(0, start) + value + current.substring(end + 1);
    }
    return str;
  }
}
