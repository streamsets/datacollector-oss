/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.dataCollector.execution.snapshot.common.SnapshotData;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.StageOutputJson;
import com.streamsets.pipeline.runner.StageOutput;

import java.util.ArrayList;
import java.util.List;

public class SnapshotDataJson {

  private final SnapshotData snapshotData;

  public SnapshotDataJson(
    List<List<StageOutputJson>> snapshotJson) {
    List<List<StageOutput>> result = new ArrayList<>(snapshotJson.size());
    for(List<StageOutputJson> snapshot : snapshotJson) {
      result.add(BeanHelper.unwrapStageOutput(snapshot));
    }
    this.snapshotData = new SnapshotData(result);
  }

  public SnapshotDataJson(SnapshotData snapshotData) {
    Utils.checkNotNull(snapshotData, "snapshotData");
    this.snapshotData = snapshotData;
  }

  public List<List<StageOutputJson>> getSnapshotBatches() {
    List<List<StageOutputJson>> result = new ArrayList<>(snapshotData.getSnapshotBatches().size());
    for(List<StageOutput> snapshot : snapshotData.getSnapshotBatches()) {
      result.add(BeanHelper.wrapStageOutput(snapshot));
    }
    return result;
  }

  @JsonIgnore
  public SnapshotData getSnapshotData() {
    return snapshotData;
  }
}
