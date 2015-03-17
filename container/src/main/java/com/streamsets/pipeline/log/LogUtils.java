/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.log;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.main.RuntimeInfo;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class LogUtils {
  public static final String LOG4J_FILE_ATTR = "log4j.filename";
  public static final String LOG4J_APPENDER_STREAMSETS_FILE_PROPERTY = "log4j.appender.streamsets.File";

  public static String getLogFile(RuntimeInfo runtimeInfo) throws IOException {
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
