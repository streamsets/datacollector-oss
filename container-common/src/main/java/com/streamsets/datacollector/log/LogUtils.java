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
package com.streamsets.datacollector.log;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.log.Constants;
import com.streamsets.pipeline.lib.parser.log.Log4jHelper;
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.dictionary.GrokDictionary;
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.util.Grok;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class LogUtils {
  public static final String LOG4J_FILE_ATTR = "log4j.filename";
  public static final String LOG4J_APPENDER_STREAMSETS_FILE_PROPERTY = "log4j.appender.streamsets.File";
  public static final String LOG4J_APPENDER_STREAMSETS_LAYOUT_CONVERSION_PATTERN =
      "log4j.appender.streamsets.layout.ConversionPattern";
  public static final String LOG4J_APPENDER_STDERR_LAYOUT_CONVERSION_PATTERN = "log4j.appender.stderr.layout.ConversionPattern";
  public static final String LOG4J_GROK_ATTR = "log4j.grok";
  public static final String LOG4J_CONVERSION_PATTERN = "%d{ISO8601} [user:%X{s-user}] [pipeline:%X{s-entity}] [runner:%X{s-runner}][thread:%t] %-5p %c{1} - %m%n";

  private LogUtils() {}

  public static String getLogFile(RuntimeInfo runtimeInfo) throws IOException {
    if(Boolean.getBoolean("sdc.transient-env")) {
      // running under mesos
      // TODO - find a better way, as this logs all spark stuff under sdc pipeline logs
      String mesosRootDir = System.getenv("MESOS_DIRECTORY");
      if (mesosRootDir!=null) {
        return new File(mesosRootDir, "stderr").getAbsolutePath();
      }
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
            throw new IOException(Utils.format(
                "Property '{}' is not defined in {}. No log file is configured for display.",
                LOG4J_APPENDER_STREAMSETS_FILE_PROPERTY,
                log4jConfig
            ));
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

  public static Grok getLogGrok(RuntimeInfo runtimeInfo) throws IOException, DataParserException {
    Grok logFileGrok = runtimeInfo.getAttribute(LOG4J_GROK_ATTR);
    String logPattern;
    if(logFileGrok == null) {
      if(Boolean.getBoolean("sdc.transient-env")) {
        // hack for Worker SDC until we use single log4j property for Standalone and Worker SDC
        logPattern = LOG4J_CONVERSION_PATTERN;
      } else {
        URL log4jConfig = runtimeInfo.getAttribute(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR);
        if (log4jConfig != null) {
          try (InputStream is = log4jConfig.openStream()) {
            Properties props = new Properties();
            props.load(is);
            logPattern = props.getProperty(LOG4J_APPENDER_STREAMSETS_LAYOUT_CONVERSION_PATTERN);
            if (logPattern == null) {
              logPattern = props.getProperty(LOG4J_APPENDER_STDERR_LAYOUT_CONVERSION_PATTERN);
            }
          }
        } else {
          throw new IOException(Utils.format("RuntimeInfo does not has attribute '{}'",
              RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR));
        }
      }

      if (logPattern != null) {
        String grokPattern = Log4jHelper.translateLog4jLayoutToGrok(logPattern);
        GrokDictionary grokDictionary = new GrokDictionary();
        try(
          InputStream grogPatterns = LogUtils.class.getClassLoader().getResourceAsStream(Constants.GROK_PATTERNS_FILE_NAME);
          InputStream javaPatterns = LogUtils.class.getClassLoader().getResourceAsStream(Constants.GROK_JAVA_LOG_PATTERNS_FILE_NAME);
        ) {
          grokDictionary.addDictionary(grogPatterns);
          grokDictionary.addDictionary(javaPatterns);
        }
        grokDictionary.bind();
        logFileGrok = grokDictionary.compileExpression(grokPattern);
        runtimeInfo.setAttribute(LOG4J_GROK_ATTR, logFileGrok);
      } else {
        throw new IllegalStateException("Cannot find log4j layout conversion pattern");
      }

    }
    return logFileGrok;
  }

  public static File[] getLogFiles(RuntimeInfo runtimeInfo) throws IOException {
    String logFile = LogUtils.getLogFile(runtimeInfo);
    File log = new File(logFile);
    File logDir = log.getParentFile();
    final String logName = log.getName();
    return logDir.listFiles((dir, name) -> name.startsWith(logName));
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
