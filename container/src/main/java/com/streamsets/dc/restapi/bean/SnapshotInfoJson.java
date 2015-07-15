/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.dc.execution.SnapshotInfo;
import com.streamsets.dc.execution.snapshot.common.SnapshotInfoImpl;

public class SnapshotInfoJson {

  private final SnapshotInfo snapshotInfo;

  @JsonCreator
  public SnapshotInfoJson(@JsonProperty("user") String user,
                          @JsonProperty("id") String id,
                          @JsonProperty("name") String name,
                          @JsonProperty("rev") String rev,
                          @JsonProperty("timeStamp") long timeStamp,
                          @JsonProperty("inProgress") boolean inProgress) {
    snapshotInfo = new SnapshotInfoImpl(user, id, name, rev, timeStamp, inProgress);
  }


  public SnapshotInfoJson(SnapshotInfo snapshotInfo) {
    this.snapshotInfo = snapshotInfo;
  }

  public String getId() {
    return snapshotInfo.getId();
  }

  public String getName() {
    return snapshotInfo.getName();
  }

  public String getRev() {
    return snapshotInfo.getRev();
  }

  public long getTimeStamp() {
    return snapshotInfo.getTimeStamp();
  }

  public String getUser() {
    return snapshotInfo.getUser();
  }

  public boolean isInProgress() {
    return snapshotInfo.isInProgress();
  }

  @JsonIgnore
  public SnapshotInfo getSnapshotInfo() {
    return snapshotInfo;
  }
}
