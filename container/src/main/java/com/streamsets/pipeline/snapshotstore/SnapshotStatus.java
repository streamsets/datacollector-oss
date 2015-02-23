/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.snapshotstore;

public class SnapshotStatus {

  private final boolean exists;
  private final boolean snapshotInProgress;

  public SnapshotStatus(boolean exists, boolean snapshotInProgress) {
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
