/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.snapshot.common;

import com.streamsets.datacollector.execution.SnapshotInfo;

public class SnapshotInfoImpl implements SnapshotInfo {

  private final String id;
  private final String name;
  private final String rev;
  private final long timestamp;
  private final String user;
  private final boolean inProgress;

  public SnapshotInfoImpl(String user, String id, String name, String rev, long timestamp, boolean inProgress) {
    this.id = id;
    this.name = name;
    this.rev = rev;
    this.timestamp = timestamp;
    this.user = user;
    this.inProgress = inProgress;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getRev() {
    return rev;
  }

  public long getTimeStamp() {
    return timestamp;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public boolean isInProgress() {
    return inProgress;
  }
}
