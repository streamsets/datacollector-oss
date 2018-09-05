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
package com.streamsets.datacollector.util;

import com.streamsets.datacollector.execution.runner.common.Constants;
import com.streamsets.pipeline.lib.log.LogConstants;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;

public class LogUtil {

  private static final String PIPELINE = "pipeline";
  private static final String DOT = ".";
  private static final String LAYOUT_PATTERN = "%m%n";
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LogUtil.class);

  private LogUtil() {}

  public static boolean registerLogger(String pipelineName, String rev, String suffix, String filename,
    Configuration configuration) {
    String loggerName = getLoggerName(pipelineName, rev, suffix);
    boolean registered;
    Logger logger = Logger.getLogger(loggerName);
    if (logger.getAppender(loggerName) != null) {
      LOG.debug("Logger '{}' already exists", loggerName);
      registered = false;
    }
    else {
      synchronized (logger) {
        if (logger.getAppender(loggerName) == null) {
          logger.setAdditivity(false);
          logger.addAppender(createRollingFileAppender(loggerName, filename, configuration));
          registered = true;
        } else {
          LOG.debug("Logger '{}' already exists", loggerName);
          registered = false;
        }
      }
    }
    return registered;
  }

  public static void log(String pipelineName, String rev, String suffix, String message) {
    String loggerName = getLoggerName(pipelineName, rev, suffix);
    Logger.getLogger(loggerName).error(message);
  }

  public static void resetRollingFileAppender(String pipeline, String rev, String suffix) {
    String loggerName = getLoggerName(pipeline, rev, suffix);
    Logger logger =Logger.getLogger(loggerName);
    Appender logAppender = logger.getAppender(loggerName);
    if (logAppender != null) {
      logAppender.close();
    }
    logger.removeAppender(loggerName);
  }

  static String getLoggerName(String pipelineName, String rev, String suffix) {
    return PIPELINE + DOT + pipelineName + DOT + rev + DOT + suffix;
  }

  private static Appender createRollingFileAppender(String loggerName, String filename, Configuration configuration) {
    PatternLayout layout = new PatternLayout(LAYOUT_PATTERN);
    RollingFileAppender appender;
    try {
      appender = new RollingFileAppender(layout, filename, true);
      //Note that the rolling appender creates the log file in the specified location eagerly
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    int maxBackupIndex = configuration.get(Constants.MAX_BACKUP_INDEX_KEY,
      Constants.MAX_BACKUP_INDEX_DEFAULT);
    appender.setMaxBackupIndex(maxBackupIndex);
    String maxFileSize = configuration.get(Constants.MAX_ERROR_FILE_SIZE_KEY,
      Constants.MAX_ERROR_FILE_SIZE_DEFAULT);
    appender.setMaxFileSize(maxFileSize);
    appender.setName(loggerName);

    return appender;
  }

  public static void injectPipelineInMDC(String pipelineTitle, String pipelineId) {
    MDC.put(LogConstants.ENTITY, pipelineTitle + "/" + pipelineId);
  }
}
