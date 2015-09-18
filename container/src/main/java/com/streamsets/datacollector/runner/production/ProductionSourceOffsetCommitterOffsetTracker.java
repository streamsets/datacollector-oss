/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.runner.production;

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.impl.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class ProductionSourceOffsetCommitterOffsetTracker implements SourceOffsetTracker {
  private static final Logger LOG = LoggerFactory.getLogger(ProductionSourceOffsetCommitterOffsetTracker.class);
  private final OffsetCommitter offsetCommitter;
  private final File offsetFile;
  private String newOffset = ""; // not null to ensure at least one pass

  public ProductionSourceOffsetCommitterOffsetTracker(String name, String revision, RuntimeInfo runtimeInfo,
                                                      OffsetCommitter offsetCommitter) {
    this.offsetCommitter = offsetCommitter;
    offsetFile = OffsetFileUtil.getPipelineOffsetFile(runtimeInfo, name, revision);
    createOffsetFileIfRequired();
  }

  private void createOffsetFileIfRequired() {
    if(!offsetFile.exists()) {
      try {
        offsetFile.createNewFile();
      } catch (IOException e) {
        throw new RuntimeException(Utils.format("Could not create file '{}'", offsetFile.getAbsolutePath()));
      }
    }
  }

  @Override
  public boolean isFinished() {
    return newOffset == null;
  }

  @Override
  public String getOffset() {
    return newOffset;
  }

  @Override
  public void setOffset(String offset) {
    this.newOffset = offset;
  }

  @Override
  public void commitOffset() {
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Commit offset '{}'", newOffset);
      }
      offsetCommitter.commit(newOffset);
      offsetFile.setLastModified(System.currentTimeMillis());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public long getLastBatchTime() {
    return offsetFile.lastModified();
  }

}
