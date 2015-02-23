/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.snapshotstore.Snapshot;

import java.util.List;

public class SnapshotJson {

  private final Snapshot snapshot;

  @JsonCreator
  public SnapshotJson(
    @JsonProperty("snapshot") List<StageOutputJson> snapshotJson) {
    this.snapshot = new Snapshot(BeanHelper.unwrapStageOutput(snapshotJson));
  }

  public SnapshotJson(Snapshot snapshot) {
    this.snapshot = snapshot;
  }

  public List<StageOutputJson> getSnapshot() {
    return BeanHelper.wrapStageOutput(snapshot.getSnapshot());
  }

  @JsonIgnore
  public Snapshot getSnapshotObj() {
    return snapshot;
  }
}
