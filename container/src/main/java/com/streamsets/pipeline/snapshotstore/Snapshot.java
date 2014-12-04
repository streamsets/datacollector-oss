/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.snapshotstore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.runner.StageOutput;

import java.util.List;

public class Snapshot {

  private final List<StageOutput> snapshot;

  @JsonCreator
  public Snapshot(
      @JsonProperty("snapshot") List<StageOutput> snapshot) {
    this.snapshot = snapshot;
  }

  public List<StageOutput> getSnapshot() {
    return snapshot;
  }
}
