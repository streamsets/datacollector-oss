/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.snapshotstore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SnapshotStatus {

  private final boolean exists;
  private final boolean snapshotInProgress;

  @JsonCreator
  public SnapshotStatus(@JsonProperty("exists") boolean exists,
                        @JsonProperty("snapshotInProgress") boolean snapshotInProgress) {
    this.exists = exists;
    this.snapshotInProgress = snapshotInProgress;
  }

  public boolean isExists() {
    return exists;
  }

  public boolean isSnapshotInProgress() {
    return snapshotInProgress;
  }
}
