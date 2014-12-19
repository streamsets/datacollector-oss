/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.errorrecordstore.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.errorrecordstore.ErrorRecordStore;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.util.LogUtil;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileErrorRecordStore implements ErrorRecordStore {

  private static final String ERRORS_FILE = "errors.json";
  private static final String PIPELINE = "pipeline";
  private static final String ERROR = "error";
  private static final String RECORD = "record";
  private static final String STAGE = "stage";
  private static final String TYPE = "type";

  private final RuntimeInfo runtimeInfo;
  private final com.streamsets.pipeline.util.Configuration configuration;

  public FileErrorRecordStore(RuntimeInfo runtimeInfo, com.streamsets.pipeline.util.Configuration configuration) {
    this.configuration = configuration;
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  public void storeErrorRecords(String pipelineName, String rev, Map<String, List<Record>> errorRecords) {
    for(Map.Entry<String, List<Record>> e : errorRecords.entrySet()) {
      for(Record r : e.getValue()) {
        try {
          writeError(pipelineName, rev, e.getKey(), r, RECORD);
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
        writeError(pipelineName, rev, e.getKey(), e.getValue(), PIPELINE);
      } catch (JsonProcessingException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Override
  public void deleteErrors(String pipelineName, String rev) {
    for(File f : getErrorFiles(pipelineName, rev)) {
      f.delete();
    }
    LogUtil.resetRollingFileAppender(pipelineName, rev, ERROR, getErrorFileName(pipelineName, rev), configuration);
  }

  @Override
  public InputStream getErrors(String pipelineName, String rev) {
    if(getErrorsFile(pipelineName, rev).exists()) {
      try {
        return new FileInputStream(getErrorsFile(pipelineName, rev));
      } catch (FileNotFoundException e) {
        return null;
      }
    }
    return null;
  }

  @Override
  public void register(String pipelineName, String rev) {
    LogUtil.registerLogger(pipelineName, rev, ERROR, getErrorFileName(pipelineName, rev), configuration);
  }

  private String getErrorFileName(String pipelineName, String rev) {
    return getErrorsFile(pipelineName, rev).getAbsolutePath();
  }

  private File getErrorsFile(String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), ERRORS_FILE);
  }

  private File[] getErrorFiles(String pipelineName, String rev) {
    //RollingFileAppender creates backup files when the error files reach the size limit.
    //The backup files are of the form errors.json.1, errors.json.2 etc
    //Need to delete all the backup files
    File pipelineDir = PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev);
    File[] errorFiles = pipelineDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if(name.contains(ERRORS_FILE)) {
          return true;
        }
        return false;
      }
    });
    return errorFiles;
  }

  private void writeError(String pipelineName, String rev, String stageName, Object error, String type)
      throws JsonProcessingException {
    Map<String, Object> toWrite = new HashMap<>();
    toWrite.put(STAGE, stageName);
    toWrite.put(TYPE, type);
    toWrite.put(ERROR, error);
    LogUtil.log(pipelineName, rev, ERROR, ObjectMapperFactory.get().writeValueAsString(toWrite));
  }

}
