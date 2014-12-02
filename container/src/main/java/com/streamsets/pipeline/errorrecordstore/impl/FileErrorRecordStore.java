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
package com.streamsets.pipeline.errorrecordstore.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.errorrecordstore.ErrorRecordStore;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileErrorRecordStore implements ErrorRecordStore {

  private static final Logger LOG = LoggerFactory.getLogger(FileErrorRecordStore.class);
  private static final int MAX_FILE_SIZE = 1073741824;

  private static final String ERROR_RECORDS_FILE = "errorRecords.json";
  private static final String ERROR_RECORDS_DIR = "runInfo";

  private File errorRecordsBaseDir;
  private final ObjectMapper json;
  /*Remember pipelines which have exceeded error record limit. This helps log warning only the first time. Otherwise
  * the log size can grow very large*/
  private final Set<String> pipelinesExceedingErrorRecordLimit;

  public FileErrorRecordStore(RuntimeInfo runtimeInfo) {
    this.errorRecordsBaseDir = new File(runtimeInfo.getDataDir(), ERROR_RECORDS_DIR);
    json = ObjectMapperFactory.get();
    json.enable(SerializationFeature.INDENT_OUTPUT);
    pipelinesExceedingErrorRecordLimit = new HashSet<>();
  }

  @Override
  public void storeErrorRecords(String pipelineName, String rev, Map<String, List<Record>> errorRecords) {
    for(Map.Entry<String, List<Record>> entry : errorRecords.entrySet()) {
      File errorRecordFile = getErrorRecordFile(pipelineName, rev, entry.getKey());
      if (errorRecordFile.exists() && errorRecordFile.length() > MAX_FILE_SIZE) {
        if(!pipelinesExceedingErrorRecordLimit.contains(pipelineName)) {
          //Log the warning only for the very first time.
          LOG.warn("Exceeded the error record file size limit. " +
              "Records in error will not be written to file from this point onwards.");
          pipelinesExceedingErrorRecordLimit.add(pipelineName);
        }
        return;
      }
      try {
        json.writeValue(new FileOutputStream(errorRecordFile, true /*append*/), entry.getValue());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void deleteErrorRecords(String pipelineName, String rev, String stageInstanceName) {
    File errorRecordFile = getErrorRecordFile(pipelineName, rev, stageInstanceName);
    if(!errorRecordFile.exists()) {
      LOG.warn("No error records found for stage instance {} in pipeline {}", stageInstanceName, pipelineName);
    }
    LOG.info("Deleting error records for stage instance {} in pipeline {}", stageInstanceName, pipelineName);
    errorRecordFile.delete();
    pipelinesExceedingErrorRecordLimit.remove(pipelineName);
    LOG.info("Deleted error records for stage instance {} in pipeline {}", stageInstanceName, pipelineName);
  }

  @Override
  public InputStream getErrorRecords(String pipelineName, String rev, String stageInstanceName) {
    if(getErrorRecordFile(pipelineName, rev, stageInstanceName).exists()) {
      try {
        return new FileInputStream(getErrorRecordFile(pipelineName, rev, stageInstanceName));
      } catch (FileNotFoundException e) {
        LOG.warn(e.getMessage());
        return null;
      }
    }
    return null;
  }

  private File getErrorRecordFile(String pipelineName, String rev, String stageInstanceName) {
    return new File(getPipelineDir(pipelineName), stageInstanceName + "-" + ERROR_RECORDS_FILE);
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
}
