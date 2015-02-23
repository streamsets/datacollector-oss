/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SnapshotStatusJson {

  private final com.streamsets.pipeline.snapshotstore.SnapshotStatus snapshotStatus;

  @JsonCreator
  public SnapshotStatusJson(@JsonProperty("exists") boolean exists,
                            @JsonProperty("snapshotInProgress") boolean snapshotInProgress) {
    this.snapshotStatus = new com.streamsets.pipeline.snapshotstore.SnapshotStatus(exists, snapshotInProgress);
  }

  public SnapshotStatusJson(com.streamsets.pipeline.snapshotstore.SnapshotStatus snapshotStatus) {
    this.snapshotStatus = snapshotStatus;
  }

  public boolean isExists() {
    return snapshotStatus.isExists();
  }

  public boolean isSnapshotInProgress() {
    return snapshotStatus.isSnapshotInProgress();
  }

  @JsonIgnore
  public com.streamsets.pipeline.snapshotstore.SnapshotStatus getSnapshotStatus() {
    return snapshotStatus;
  }
}