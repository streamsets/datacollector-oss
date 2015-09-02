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
package com.streamsets.datacollector.runner.production;

import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.SourceOffsetJson;
import com.streamsets.datacollector.runner.SourceOffsetTracker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ProductionSourceOffsetTracker implements SourceOffsetTracker {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionSourceOffsetTracker.class);

  private static final String DEFAULT_OFFSET = null;

  private String currentOffset;
  private String newOffset;
  private boolean finished;
  private final String pipelineName;
  private final String rev;
  private final RuntimeInfo runtimeInfo;

  @Inject
  public ProductionSourceOffsetTracker( @Named("name") String pipelineName,  @Named("rev") String rev, RuntimeInfo runtimeInfo) {
    this.pipelineName = pipelineName;
    this.rev = rev;
    this.runtimeInfo = runtimeInfo;
    this.currentOffset = getSourceOffset(pipelineName, rev).getOffset();
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public String getOffset() {
    return currentOffset;
  }

  @Override
  public void setOffset(String offset) {
    this.newOffset = offset;
  }

  @Override
  public void commitOffset() {
    commitOffset(pipelineName, rev);
  }

  public void commitOffset(String pipelineName, String rev) {
    currentOffset = newOffset;
    finished = (currentOffset == null);
    newOffset = null;
    saveOffset(pipelineName, rev, new SourceOffset(currentOffset));
  }

  public SourceOffset getSourceOffset(String pipelineName, String rev) {
    File pipelineOffsetFile = OffsetFileUtil.getPipelineOffsetFile(runtimeInfo, pipelineName, rev);
    SourceOffset sourceOffset;
    if (pipelineOffsetFile.exists() && pipelineOffsetFile.length() != 0) {
      // offset file exists, read from it
      try (InputStream is = new DataStore(pipelineOffsetFile).getInputStream()) {
        SourceOffsetJson sourceOffsetJson = ObjectMapperFactory.get().readValue(is, SourceOffsetJson.class);
        sourceOffset = BeanHelper.unwrapSourceOffset(sourceOffsetJson);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      sourceOffset = new SourceOffset(DEFAULT_OFFSET);
      saveOffset(pipelineName, rev, sourceOffset);
    }
    return sourceOffset;
  }

  public void resetOffset(String pipelineName, String rev) {
    saveOffset(pipelineName, rev, new SourceOffset(DEFAULT_OFFSET));
  }

  private void saveOffset(String pipelineName, String rev, SourceOffset s) {
    LOG.debug("Saving offset {} for pipeline {}", s.getOffset(), pipelineName);
    try (OutputStream os = new DataStore(OffsetFileUtil.getPipelineOffsetFile(runtimeInfo,
          pipelineName, rev)).getOutputStream()) {
      ObjectMapperFactory.get().writeValue((os), BeanHelper.wrapSourceOffset(s));
    } catch (IOException e) {
      LOG.error("Failed to save offset value {}. Reason {}", s.getOffset(), e.toString(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getLastBatchTime() {
    return OffsetFileUtil.getPipelineOffsetFile(runtimeInfo, pipelineName, rev).lastModified();
  }
}