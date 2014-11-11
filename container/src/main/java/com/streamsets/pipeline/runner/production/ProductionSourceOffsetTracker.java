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
import com.streamsets.pipeline.agent.RuntimeInfo;
import com.streamsets.pipeline.runner.SourceOffsetTracker;

import java.io.File;
import java.io.IOException;

public class ProductionSourceOffsetTracker implements SourceOffsetTracker {

  static final String DEFAULT_PIPELINE_NAME = "xyz";
  private static final String OFFSET_FILE = "offset.json";
  private static final String DEFAULT_OFFSET = null;

  private final RuntimeInfo runtimeInfo;
  private File offsetDir;
  private ObjectMapper json;

  private String currentOffset;
  private String newOffset;
  private boolean finished;

  public ProductionSourceOffsetTracker(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
    this.offsetDir = new File(runtimeInfo.getDataDir(), "runInfo" + File.separator + DEFAULT_PIPELINE_NAME);
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
      if(getOffsetFile().exists()) {
        //offset file already exists. read the previous offset from file.
        try {
          SourceOffset sourceOffset = json.readValue(getOffsetFile(), SourceOffset.class);
          if(sourceOffset != null) {
            currentOffset = sourceOffset.getOffset();
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        //Offset file does not exist.
        //persist default state
        persistStateIntoOffsetFile(DEFAULT_OFFSET);
      }
    } else {
      //runInfo dir and offset file do not exist
      //create dir, offset file and then persist default state which is NOT_RUNNING
      if(!offsetDir.mkdirs()) {
        throw new RuntimeException(String.format("Could not create directory '%s'", offsetDir.getAbsolutePath()));
      }
      persistStateIntoOffsetFile(DEFAULT_OFFSET);
    }

    return currentOffset;
  }

  @Override
  public void setOffset(String newOffset) {
    this.newOffset = newOffset;
  }

  private File getOffsetFile() {
    return new File(offsetDir, OFFSET_FILE);
  }

  @Override
  public void commitOffset() {
    currentOffset = newOffset;
    finished = (currentOffset == null);
    newOffset = null;

    //persist current offset
    if (!offsetDir.exists()) {
      if (!offsetDir.mkdirs()) {
        throw new RuntimeException(String.format("Could not create directory '%s'", offsetDir.getAbsolutePath()));
      }
    } else {
      //persist default state
      SourceOffset s = new SourceOffset(DEFAULT_PIPELINE_NAME, currentOffset);
      try {
        json.writeValue(getOffsetFile(), s);
      } catch (IOException e) {
        //TODO throw correct exception and localize
        throw new RuntimeException(e);
      }
    }
  }

  private void persistStateIntoOffsetFile(String defaultOffset) {
    SourceOffset s = new SourceOffset(DEFAULT_PIPELINE_NAME, DEFAULT_OFFSET);
    try {
      json.writeValue(getOffsetFile(), s);
    } catch (IOException e) {
      //TODO throw corret exception and localize
      throw new RuntimeException(e);
    }
  }


}
