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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class ProductionSourceOffsetTracker implements SourceOffsetTracker {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionSourceOffsetTracker.class);

  static final String DEFAULT_PIPELINE_NAME = "xyz";
  private static final String OFFSET_FILE = "offset.json";
  private static final String OFFSET_DIR = "runInfo";
  private static final String DEFAULT_OFFSET = null;

  private File offsetDir;
  private File offsetFile;
  private ObjectMapper json;
  private String currentOffset;
  private String newOffset;
  private boolean finished;

  public ProductionSourceOffsetTracker(RuntimeInfo runtimeInfo) {
    this.offsetDir = new File(runtimeInfo.getDataDir(),
        OFFSET_DIR + File.separator + DEFAULT_PIPELINE_NAME);
    this.offsetFile = new File(offsetDir, OFFSET_FILE);
    json = new ObjectMapper();
    json.enable(SerializationFeature.INDENT_OUTPUT);
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public String getOffset() {
    currentOffset = DEFAULT_OFFSET;
    //offset directory already exists?
    if(offsetDir.exists()) {
      //offset file also exists?
      if(offsetFile.exists()) {
        //offset file already exists. read the previous offset from file.
        try {
          SourceOffset sourceOffset = json.readValue(offsetFile, SourceOffset.class);
          if(sourceOffset != null) {
            currentOffset = sourceOffset.getOffset();
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        //Offset file does not exist.
        //persist default state
        saveOffset(DEFAULT_OFFSET);
      }
    } else {
      //runInfo dir and offset file do not exist
      //create dir, offset file and then persist default state which is NOT_RUNNING
      if(!offsetDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", offsetDir.getAbsolutePath()));
      }
      saveOffset(DEFAULT_OFFSET);
    }

    return currentOffset;
  }

  @Override
  public void setOffset(String newOffset) {
    this.newOffset = newOffset;
  }

  @Override
  public void commitOffset() {
    currentOffset = newOffset;
    finished = (currentOffset == null);
    newOffset = null;

    //persist current offset
    if (!offsetDir.exists()) {
      if (!offsetDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", offsetDir.getAbsolutePath()));
      }
    }
    //persist default state
    saveOffset(currentOffset);
  }

  private void saveOffset(String offset) {
    SourceOffset s = new SourceOffset(offset);
    try {
      json.writeValue(offsetFile, s);
    } catch (IOException e) {
      LOG.error(Utils.format("Failed to save offset value {}. Reason {}", s.getOffset(), e.getMessage()));
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  public File getOffsetFile() {
    return offsetFile;
  }

}
