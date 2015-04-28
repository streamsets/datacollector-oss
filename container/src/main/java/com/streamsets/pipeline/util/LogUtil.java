/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.prodmanager.Constants;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LogUtil {

  private static final String PIPELINE = "pipeline";
  private static final String DOT = ".";
  private static final String LAYOUT_PATTERN = "%m%n";

  /*cache all registered loggers, otherwise each time a new pipeline is created, a new rolling appender is added
  * to the logger*/
  private static final Map<String, String> registeredLoggers = new HashMap<>();

  public static void registerLogger(String pipelineName, String rev, String suffix, String filename,
                             Configuration configuration) {
    String loggerName = getLoggerName(pipelineName, rev, suffix);
    if(registeredLoggers.containsKey(loggerName)) {
      if(!registeredLoggers.get(loggerName).equals(filename)) {
        throw new RuntimeException(Utils.format("The pipeline '{}' revision '{}' suffix '{}' is already registered.",
          pipelineName, rev, suffix));
      }
      //no op
      return;
    }
    Logger logger = Logger.getLogger(loggerName);
    logger.setAdditivity(false);
    logger.addAppender(createRollingFileAppender(loggerName, filename, configuration));
    registeredLoggers.put(loggerName, filename);
  }

  public static void log(String pipelineName, String rev, String suffix, String message) {
    String loggerName = getLoggerName(pipelineName, rev, suffix);
    Logger.getLogger(loggerName).error(message);
  }

  public static void resetRollingFileAppender(String pipeline, String rev, String suffix, String filename,
                                              Configuration configuration) {
    String loggerName = getLoggerName(pipeline, rev, suffix);
    Logger logger =Logger.getLogger(loggerName);
    logger.removeAppender(loggerName);
    logger.addAppender(createRollingFileAppender(loggerName, filename, configuration));
  }

  private static String getLoggerName(String pipelineName, String rev, String suffix) {
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

  public static void unregisterAllLoggers() {
    for(Map.Entry<String, String> entry : registeredLoggers.entrySet()) {
      Logger logger = Logger.getLogger(entry.getKey());
      logger.removeAppender(entry.getKey());
    }
    registeredLoggers.clear();
  }
}
