/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.snapshotstore;

import com.streamsets.pipeline.runner.StageOutput;

import java.util.List;

public class Snapshot {

  private final List<StageOutput> snapshot;

  public Snapshot(List<StageOutput> snapshot) {
    this.snapshot = snapshot;
  }

  public List<StageOutput> getSnapshot() {
    return snapshot;
  }
}
