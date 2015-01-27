/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.observerstore.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.observerstore.SamplingStore;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.LogUtil;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class FileSamplingStore implements SamplingStore {

  private static final Logger LOG = LoggerFactory.getLogger(FileSamplingStore.class);

  private static final String SAMPLE = "sample";
  private static final String SAMPLED_RECORDS_FILE = "sampledRecords.json";

  private final RuntimeInfo runtimeInfo;
  private final Configuration configuration;

  public FileSamplingStore(RuntimeInfo runtimeInfo, Configuration configuration) {
    this.configuration = configuration;
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  public void storeSampledRecords(String pipelineName, String rev, Map<String, List<Record>> sampledRecords) {
    try {
      LogUtil.log(pipelineName, rev, SAMPLE, ObjectMapperFactory.get().writeValueAsString(sampledRecords));
    } catch (JsonProcessingException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void deleteSampledRecords(String pipelineName, String rev) {
    for(File f : getSampledRecordsFiles(pipelineName, rev)) {
      f.delete();
    }
    LogUtil.resetRollingFileAppender(pipelineName, rev, SAMPLE, getSampledRecordsFileName(pipelineName, rev),
      configuration);
  }

  @Override
  public InputStream getSampledRecords(String pipelineName, String rev) {
    if(getSampledRecordsFile(pipelineName, rev).exists()) {
      try {
        return new FileInputStream(getSampledRecordsFile(pipelineName, rev));
      } catch (FileNotFoundException e) {
        return null;
      }
    }
    return null;
  }

  @Override
  public void register(String pipelineName, String rev) {
    LogUtil.registerLogger(pipelineName, rev, SAMPLE, getSampledRecordsFileName(pipelineName, rev), configuration);
  }

  private String getSampledRecordsFileName(String pipelineName, String rev) {
    return getSampledRecordsFile(pipelineName, rev).getAbsolutePath();
  }

  private File getSampledRecordsFile(String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), SAMPLED_RECORDS_FILE);
  }

  private File[] getSampledRecordsFiles(String pipelineName, String rev) {
    //RollingFileAppender creates backup files when the error files reach the size limit.
    //The backup files are of the form errors.json.1, errors.json.2 etc
    //Need to delete all the backup files
    File pipelineDir = PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev);
    File[] samplesFile = pipelineDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if(name.contains(SAMPLED_RECORDS_FILE)) {
          return true;
        }
        return false;
      }
    });
    return samplesFile;
  }

}
