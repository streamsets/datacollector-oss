/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.runner.SourceOffsetTracker;

import java.io.File;
import java.io.IOException;

public class ProductionSourceOffsetCommitterOffsetTracker implements SourceOffsetTracker {

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
    if (newOffset != null) {
      try {
        offsetCommitter.commit(newOffset);
        offsetFile.setLastModified(System.currentTimeMillis());
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Override
  public long getLastBatchTime() {
    return offsetFile.lastModified();
  }

}
