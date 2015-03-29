/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Date;

public class SnapshotInfoJson {
  private final com.streamsets.pipeline.snapshotstore.SnapshotInfo snapshotInfo;

  @JsonCreator
  public SnapshotInfoJson(
    @JsonProperty("pipelineName") String pipelineName,
    @JsonProperty("snapshotName") String snapshotName,
    @JsonProperty("captured") Date captured) {
    this.snapshotInfo = new com.streamsets.pipeline.snapshotstore.SnapshotInfo(pipelineName, snapshotName, captured);
  }

  public SnapshotInfoJson(com.streamsets.pipeline.snapshotstore.SnapshotInfo snapshotInfo) {
    Utils.checkNotNull(snapshotInfo, "snapshotInfo");
    this.snapshotInfo = snapshotInfo;
  }

  public String getPipelineName() {
    return snapshotInfo.getPipelineName();
  }

  public String getSnapshotName() {
    return snapshotInfo.getSnapshotName();
  }

  public Date getCaptured() {
    return snapshotInfo.getCaptured();
  }

  @JsonIgnore
  public com.streamsets.pipeline.snapshotstore.SnapshotInfo getSnapshotInfo() {
    return snapshotInfo;
  }

}
