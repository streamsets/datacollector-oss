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
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.util.JsonFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class ProductionSourceOffsetTracker implements SourceOffsetTracker {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionSourceOffsetTracker.class);

  private static final String TEMP_OFFSET_FILE = "offset.json.tmp";
  private static final String OFFSET_FILE = "offset.json";
  private static final String OFFSET_DIR = "runInfo";
  private static final String DEFAULT_OFFSET = null;

  private File offsetBaseDir;
  private JsonFileUtil<SourceOffset> json;
  private String currentOffset;
  private String newOffset;
  private boolean finished;
  private final String pipelineName;

  public ProductionSourceOffsetTracker(String pipelineName, RuntimeInfo runtimeInfo) {
    this.offsetBaseDir = new File(runtimeInfo.getDataDir(), OFFSET_DIR);
    json = new JsonFileUtil<>();
    this.pipelineName = pipelineName;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public String getOffset() {
    return getSourceOffset(pipelineName).getOffset();
  }

  @Override
  public void setOffset(String offset) {
    this.newOffset = offset;
  }

  @Override
  public void commitOffset() {
    commitOffset(pipelineName);
  }

  public void commitOffset(String pipelineName) {
    currentOffset = newOffset;
    finished = (currentOffset == null);
    newOffset = null;
    saveOffset(pipelineName, new SourceOffset(currentOffset));
  }


  public SourceOffset getSourceOffset(String pipelineName) {
    File pipelineOffsetFile = getPipelineOffsetFile(pipelineName);
    SourceOffset sourceOffset;
    if(pipelineOffsetFile.exists()) {
      //offset file exists, read from it
      try {
        sourceOffset = json.readObjectFromFile(pipelineOffsetFile, SourceOffset.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      sourceOffset = new SourceOffset(DEFAULT_OFFSET);
      saveOffset(pipelineName, sourceOffset);
    }
    return sourceOffset;
  }

  private void saveOffset(String pipelineName, SourceOffset s) {
    LOG.debug("Saving offset {} for pipeline {}", s.getOffset(), pipelineName);
    try {
      json.writeObjectToFile(getPipelineOffsetTempFile(pipelineName), getPipelineOffsetFile(pipelineName), s);
    } catch (IOException e) {
      LOG.error(Utils.format("Failed to save offset value {}. Reason {}", s.getOffset(), e.getMessage()));
      throw new RuntimeException(e);
    }
  }

  private File getPipelineOffsetFile(String name) {
    return new File(getPipelineDir(name), OFFSET_FILE);
  }

  private File getPipelineOffsetTempFile(String name) {
    return new File(getPipelineDir(name), TEMP_OFFSET_FILE);
  }

  private File getPipelineDir(String name) {
    File pipelineDir = new File(offsetBaseDir, name);
    if(!pipelineDir.exists()) {
      if(!pipelineDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", pipelineDir.getAbsolutePath()));
      }
    }
    return pipelineDir;
  }

}
