/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.runner;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.record.RecordImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipeBatch {
  private final SourceOffsetTracker offsetTracker;
  private final Map<String, List<Record>> fullPayload;
  private final Set<String> processedStages;
  private final List<StageOutput> stageOutputSnapshot;

  public PipeBatch(SourceOffsetTracker offsetTracker, boolean snapshotStages) {
    this.offsetTracker = offsetTracker;
    fullPayload = new HashMap<String, List<Record>>();
    processedStages = new HashSet<String>();
    stageOutputSnapshot = (snapshotStages) ? new ArrayList<StageOutput>() : null;
  }

  public String getPreviousOffset() {
    return offsetTracker.getOffset();
  }

  public void setNewOffset(String offset) {
    offsetTracker.setOffset(offset);
  }

  public BatchImpl getBatch(final Pipe pipe) {
    List<Record> records = new ArrayList<Record>();
    for (String inputLane : pipe.getInputLanes()) {
      records.addAll(fullPayload.remove(inputLane));
    }
    return new BatchImpl(offsetTracker, records);
  }

  public BatchMakerImpl startStage(StagePipe pipe) {
    String stageName = pipe.getStage().getInfo().getInstanceName();
    Preconditions.checkState(!processedStages.contains(stageName), String.format(
        "The stage '%s' has been processed already", stageName));
    processedStages.add(stageName);
    for (String output : pipe.getOutputLanes()) {
      fullPayload.put(output, null);
    }
    return new BatchMakerImpl(pipe, stageOutputSnapshot != null);
  }

  public void completeStage(BatchMakerImpl batchMaker) {
    StagePipe pipe = batchMaker.getStagePipe();
    Map<String, List<Record>> stageOutput = batchMaker.getStageOutput();
    // convert lane names from stage naming to pipe naming when adding to the payload
    // leveraging the fact that the stage output lanes and the pipe output lanes are in the same order
    List<String> stageLaneNames = pipe.getStage().getConfiguration().getOutputLanes();
    for (int i = 0; i < stageLaneNames.size() ; i++) {
      String stageLaneName = stageLaneNames.get(i);
      String pipeLaneName = pipe.getOutputLanes().get(i);
      fullPayload.put(pipeLaneName, stageOutput.get(stageLaneName));
    }
    if (stageOutputSnapshot != null) {
      stageOutputSnapshot.add(new StageOutput(pipe.getStage().getInfo().getInstanceName(),
                              batchMaker.getStageOutputSnapshot()));
    }
  }

  public void commitOffset() {
    offsetTracker.commitOffset();
  }

  public Map<String, List<Record>> getPipeLanesSnapshot(List<String> pipeLanes) {
    Map<String, List<Record>> snapshot = new HashMap<String, List<Record>>();
    for (String pipeLane : pipeLanes) {
      snapshot.put(pipeLane, createSnapshot(fullPayload.get(pipeLane)));
    }
    return snapshot;
  }

  private List<Record> createSnapshot(List<Record> records) {
    List<Record> list = new ArrayList<Record>(records.size());
    for (Record record : records) {
      list.add(((RecordImpl) record).createSnapshot());
    }
    return list;
  }

  public List<StageOutput> getOutputSnapshot() {
    return stageOutputSnapshot;
  }

  public void moveLane(String inputLane, String outputLane) {
    fullPayload.put(outputLane, Preconditions.checkNotNull(fullPayload.remove(inputLane), String.format(
        "Lane '%s' does not exist", inputLane)));
  }

  public void moveLaneCloning(String inputLane, List<String> outputLanes) {
    List<Record> records = Preconditions.checkNotNull(fullPayload.remove(inputLane), String.format(
        "Lane '%s' does not exist", inputLane));
    for (String lane : outputLanes) {
      Preconditions.checkNotNull(fullPayload.containsKey(lane), String.format("Lane '%s' does not exist", lane));
      fullPayload.put(lane, createCopy(records));
    }
  }

  private List<Record> createCopy(List<Record> records) {
    List<Record> list = new ArrayList<Record>(records.size());
    for (Record record : records) {
      list.add(((RecordImpl) record).createCopy());
    }
    return list;
  }

  public void combineLanes(List<String> lanes, String to) {
    Preconditions.checkState(fullPayload.containsKey(to), "Lane '%s' does not exist");
    fullPayload.put(to, new ArrayList<Record>());
    for (String lane : lanes) {
      List<Record> records = Preconditions.checkNotNull(fullPayload.remove(lane), String.format(
          "Lane '%s' does not exist", lane));
      fullPayload.get(to).addAll(records);
    }
  }

}
