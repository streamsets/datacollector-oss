/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.datacollector.execution.snapshot.common.SnapshotData;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.pipeline.api.impl.Utils;

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
