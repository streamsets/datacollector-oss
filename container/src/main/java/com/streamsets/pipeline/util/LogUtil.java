/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.streamsets.pipeline.prodmanager.Constants;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LogUtil {

  private static final String PIPELINE = "pipeline";
  private static final String DOT = ".";
  private static final String LAYOUT_PATTERN = "%m%n";
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LogUtil.class);

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
}
