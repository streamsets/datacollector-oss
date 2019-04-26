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

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.SourceOffsetTracker;

import com.streamsets.pipeline.api.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ProductionSourceOffsetTracker implements SourceOffsetTracker {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionSourceOffsetTracker.class);
  private Map<String, String> offsets;
  private volatile long lastBatchTime;
  private boolean finished;
  private final String pipelineName;
  private final String rev;
  private final RuntimeInfo runtimeInfo;

  @Inject
  public ProductionSourceOffsetTracker(
      @Named("name") String pipelineName,
      @Named("rev") String rev,
      RuntimeInfo runtimeInfo
  ) {
    this.pipelineName = pipelineName;
    this.rev = rev;
    this.runtimeInfo = runtimeInfo;
    this.offsets = new HashMap<>(getSourceOffset(pipelineName, rev));
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public void commitOffset(String entity, String newOffset) {
    // Update last batch time
    lastBatchTime = System.currentTimeMillis();

    // Short cut when origin is committing "null" entity then they are in fact not changing anything, so we don't need
    // to synchronize on single file to write it down.
    if(entity == null) {
      return;
    }

    // Backward compatibility calculation
    if(Source.POLL_SOURCE_OFFSET_KEY.equals(entity)) {
      finished = newOffset == null;
    }

    // This object can be called from multiple threads, so we have to synchronize access to the offset map
    synchronized (offsets) {
      if (newOffset == null) {
        offsets.remove(entity);
      } else {
        offsets.put(entity, newOffset);
      }

      // Finally write new variant of the offset file
      saveOffset(pipelineName, rev, offsets);
    }
  }

  @Override
  public Map<String, String> getOffsets() {
    return Collections.unmodifiableMap(offsets);
  }

  public Map<String, String> getSourceOffset(String pipelineName, String rev) {
    return OffsetFileUtil.saveIfEmpty(runtimeInfo, pipelineName, rev);
  }

  @Override
  public void resetOffset() {
    OffsetFileUtil.resetOffsets(runtimeInfo, pipelineName, rev);
  }

  private void saveOffset(String pipelineName, String rev, Map<String, String> offset) {
    OffsetFileUtil.saveOffsets(runtimeInfo, pipelineName, rev, offset);
  }

  @Override
  public long getLastBatchTime() {
    return lastBatchTime;
  }
}
