/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.preview;

import com.streamsets.pipeline.runner.SourceOffsetTracker;

public class PreviewSourceOffsetTracker implements SourceOffsetTracker {
  private String currentOffset;
  private String newOffset;
  private boolean finished;

  public PreviewSourceOffsetTracker(String currentOffset) {
    this.currentOffset = currentOffset;
    finished = false;
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
  public void setOffset(String newOffset) {
    this.newOffset = newOffset;
  }

  @Override
  public void commitOffset() {
    currentOffset = newOffset;
    finished = (currentOffset == null);
    newOffset = null;
  }

  @Override
  public long getLastBatchTime() {
    return 0;
  }

}
