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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;

public class ProductionSourceOffsetTracker implements SourceOffsetTracker {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionSourceOffsetTracker.class);
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
    this.currentOffset = getSourceOffset(pipelineName, rev);
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
    saveOffset(pipelineName, rev, currentOffset);
  }

  public String getSourceOffset(String pipelineName, String rev) {
    return OffsetFileUtil.saveIfEmpty(runtimeInfo, pipelineName, rev);
  }

  public void resetOffset(String pipelineName, String rev) {
    OffsetFileUtil.resetOffset(runtimeInfo, pipelineName, rev);
  }

  private void saveOffset(String pipelineName, String rev, String offset) {
    LOG.debug("Saving offset {} for pipeline {}", offset, pipelineName);
    OffsetFileUtil.saveOffset(runtimeInfo, pipelineName, rev, offset);
  }

  @Override
  public long getLastBatchTime() {
    return OffsetFileUtil.getPipelineOffsetFile(runtimeInfo, pipelineName, rev).lastModified();
  }
}