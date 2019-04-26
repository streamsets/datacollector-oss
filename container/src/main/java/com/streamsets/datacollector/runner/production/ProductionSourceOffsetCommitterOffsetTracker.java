/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.impl.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Offset committer that doesn't store offsets in external system, but let's the (Pull) origin to handle the offsets
 * on it's own. Since OffsetCommitter interface is not applicable to PushSource class, this will properly work only
 * with Source implementing OffsetCommitter interface.
 */
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
        throw new RuntimeException(Utils.format("Could not create file '{}'", offsetFile.getAbsolutePath()), e);
      }
    }
  }

  @Override
  public boolean isFinished() {
    return newOffset == null;
  }

  @Override
  public void commitOffset(String entity, String newOffset) {
    Preconditions.checkArgument(Source.POLL_SOURCE_OFFSET_KEY.equals(entity), "Trying to commit offset for invalid entity: " + entity);
    this.newOffset = newOffset;

    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Commit offset '{}'", newOffset);
      }
      offsetCommitter.commit(newOffset);
      if (!offsetFile.setLastModified(System.currentTimeMillis())) {
        LOG.warn("Failed to set Last Modified on file " + offsetFile);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Map<String, String> getOffsets() {
    return Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, newOffset);
  }

  @Override
  public long getLastBatchTime() {
    return offsetFile.lastModified();
  }

  @Override
  public void resetOffset() {
    LOG.warn("Ignored request to reset offset.");
  }

}
