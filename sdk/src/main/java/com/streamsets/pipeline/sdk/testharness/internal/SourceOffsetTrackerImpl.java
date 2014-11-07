package com.streamsets.pipeline.sdk.testharness.internal;

import com.streamsets.pipeline.runner.SourceOffsetTracker;

public class SourceOffsetTrackerImpl implements SourceOffsetTracker {

  private String currentOffset;
  private String newOffset;
  private boolean finished;

  public SourceOffsetTrackerImpl(String currentOffset) {
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
}