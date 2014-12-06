/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.errorrecordstore.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.container.ErrorMessage;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.errorrecordstore.ErrorRecordStore;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.Constants;
import com.streamsets.pipeline.util.Configuration;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileErrorRecordStore implements ErrorRecordStore {

  private static final String ERRORS_FILE = "errors.json";
  private static final String ERROR_RECORDS_DIR = "runInfo";
  private static final String PIPELINE = "pipeline";
  private static final String DOT = ".";
  private static final String ERROR = "error";
  private static final String RECORD = "record";
  private static final String STAGE = "stage";
  private static final String TYPE = "type";
  private static final String LAYOUT_PATTERN = "%m%n";

  private File errorRecordsBaseDir;
  private final ObjectMapper json;
  private final Configuration configuration;

  public FileErrorRecordStore(RuntimeInfo runtimeInfo, Configuration configuration) {
    this.configuration = configuration;
    this.errorRecordsBaseDir = new File(runtimeInfo.getDataDir(), ERROR_RECORDS_DIR);
    json = ObjectMapperFactory.get();
    json.enable(SerializationFeature.INDENT_OUTPUT);
  }

  @Override
  public void storeErrorRecords(String pipelineName, String rev, Map<String, List<Record>> errorRecords) {
    for(Map.Entry<String, List<Record>> e : errorRecords.entrySet()) {
      for(Record r : e.getValue()) {
        try {
          writeError(pipelineName, e.getKey(), r, RECORD);
        } catch (JsonProcessingException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }

  @Override
  public void storeErrorMessages(String pipelineName, String rev, Map<String, List<ErrorMessage>> errorMessages) {
    for(Map.Entry<String, List<ErrorMessage>> e : errorMessages.entrySet()) {
      try {
        writeError(pipelineName, e.getKey(), e.getValue(), PIPELINE);
      } catch (JsonProcessingException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Override
  public void deleteErrors(String pipelineName, String rev) {
    if(getErrorsFile(pipelineName).exists()) {
      getErrorsFile(pipelineName).delete();
    }
  }

  @Override
  public InputStream getErrors(String pipelineName, String rev) {
    if(getErrorsFile(pipelineName).exists()) {
      try {
        return new FileInputStream(getErrorsFile(pipelineName));
      } catch (FileNotFoundException e) {
        return null;
      }
    }
    return null;
  }

  public void register(String pipelineName) {
    String loggerName = PIPELINE + DOT + pipelineName + DOT + ERROR;
    PatternLayout layout = new PatternLayout(LAYOUT_PATTERN);
    RollingFileAppender appender;
    try {
      appender = new RollingFileAppender(layout, getErrorFileName(pipelineName), true);
      //Note that the rolling appender creates the log file in the specified location eagerly
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    int maxBackupIndex = configuration.get(Constants.MAX_BACKUP_INDEX, Constants.MAX_BACKUP_INDEX_DEFAULT);
    appender.setMaxBackupIndex(maxBackupIndex);
    String maxFileSize = configuration.get(Constants.MAX_ERROR_FILE_SIZE, Constants.MAX_ERROR_FILE_SIZE_DEFAULT);
    appender.setMaxFileSize(maxFileSize);

    Logger logger = Logger.getLogger(loggerName);
    logger.addAppender(appender);
    //prevent other loggers from logging errors
    logger.setAdditivity(false);
  }

  private Logger getLogger(String pipelineName) {
    String loggerName = PIPELINE + DOT + pipelineName + DOT + ERROR;
    return Logger.getLogger(loggerName);
  }

  private String getErrorFileName(String pipelineName) {
    return getErrorsFile(pipelineName).getAbsolutePath();
  }

  private File getErrorsFile(String pipelineName) {
    return new File(getPipelineDir(pipelineName), ERRORS_FILE);
  }

  private File getPipelineDir(String name) {
    File pipelineDir = new File(errorRecordsBaseDir, name);
    if(!pipelineDir.exists()) {
      if(!pipelineDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", pipelineDir.getAbsolutePath()));
      }
    }
    return pipelineDir;
  }

  private void writeError(String pipelineName, String stageName, Object error, String type) throws JsonProcessingException {
    Map<String, Object> toWrite = new HashMap<>();
    toWrite.put(STAGE, stageName);
    toWrite.put(TYPE, type);
    toWrite.put(ERROR, error);
    getLogger(pipelineName).error(json.writeValueAsString(toWrite));
  }

}
