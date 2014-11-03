package com.clipper.stage;

import com.streamsets.pipeline.container.SourceTracker;

public class TSourceTracker implements SourceTracker {
  private boolean finished;
  private String batchId;

  public boolean isFinished() {
    return finished;
  }

  @Override
  public String getLastBatchId() {
    return batchId;
  }

  @Override
  public void udpateLastBatchId(String batchId) {
    this.batchId = batchId;
    finished = batchId == null;
  }
}