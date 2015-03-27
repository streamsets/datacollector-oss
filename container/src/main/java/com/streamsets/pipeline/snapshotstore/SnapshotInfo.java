/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.snapshotstore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class SnapshotInfo {
  private String pipelineName;
  private String snapshotName;
  private Date created;

  @JsonCreator
  public SnapshotInfo(
    @JsonProperty("pipelineName") String pipelineName,
    @JsonProperty("snapshotName") String snapshotName,
    @JsonProperty("created") Date created) {
    this.snapshotName = snapshotName;
    this.pipelineName = pipelineName;
    this.created = created;
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public String getSnapshotName() {
    return snapshotName;
  }

  public Date getCreated() {
    return created;
  }

}
