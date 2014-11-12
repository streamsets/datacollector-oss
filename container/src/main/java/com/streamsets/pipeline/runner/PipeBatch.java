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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.record.RecordImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipeBatch {
  private final SourceOffsetTracker offsetTracker;
  private final int batchSize;
  private final Map<String, List<Record>> fullPayload;
  private final Set<String> processedStages;
  private final List<StageOutput> stageOutputSnapshot;
  private final ErrorRecordSink errorRecordSink;

  public PipeBatch(SourceOffsetTracker offsetTracker, int batchSize, boolean snapshotStagesOutput) {
    this.offsetTracker = offsetTracker;
    this.batchSize = batchSize;
    fullPayload = new HashMap<String, List<Record>>();
    processedStages = new HashSet<String>();
    stageOutputSnapshot = (snapshotStagesOutput) ? new ArrayList<StageOutput>() : null;
    this.errorRecordSink = new ErrorRecordSink();
  }

  @VisibleForTesting
  Map<String, List<Record>> getFullPayload() {
    return fullPayload;
  }

  public int getBatchSize() {
    return batchSize;
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
    Preconditions.checkState(!processedStages.contains(stageName), Utils.format(
        "The stage '{}' has been processed already", stageName));
    processedStages.add(stageName);
    for (String output : pipe.getOutputLanes()) {
      fullPayload.put(output, null);
    }
    int recordAllowance = (pipe.getStage().getDefinition().getType() == StageType.SOURCE)
                          ? getBatchSize() : Integer.MAX_VALUE;
    return new BatchMakerImpl(pipe, stageOutputSnapshot != null, recordAllowance);
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
      String instanceName = pipe.getStage().getInfo().getInstanceName();
      stageOutputSnapshot.add(new StageOutput(instanceName, batchMaker.getStageOutputSnapshot(),
                                              errorRecordSink.getErrorRecords(instanceName)));
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

  public List<StageOutput> getSnapshotsOfAllStagesOutput() {
    return stageOutputSnapshot;
  }

  public ErrorRecordSink getErrorRecordSink() {
    return errorRecordSink;
  }

  public void moveLane(String inputLane, String outputLane) {
    fullPayload.put(outputLane, Preconditions.checkNotNull(fullPayload.remove(inputLane), Utils.format(
        "Lane '{}' does not exist", inputLane)));
  }

  public void moveLaneCopying(String inputLane, List<String> outputLanes) {
    List<Record> records = Preconditions.checkNotNull(fullPayload.remove(inputLane), Utils.format(
        "Lane '{}' does not exist", inputLane));
    for (String lane : outputLanes) {
      Preconditions.checkNotNull(fullPayload.containsKey(lane), Utils.format("Lane '{}' does not exist", lane));
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

  private List<String> remove(List<String> from, Collection<String> values) {
    List<String> list = new ArrayList<String>(from);
    list.removeAll(values);
    return list;
  }

  public void combineLanes(List<String> lanes, String to) {
    List<String> undefLanes = remove(lanes, fullPayload.keySet());
    Preconditions.checkState(undefLanes.isEmpty(), Utils.format("Lanes '{}' does not exist", undefLanes));
    fullPayload.put(to, new ArrayList<Record>());
    for (String lane : lanes) {
      List<Record> records = Preconditions.checkNotNull(fullPayload.remove(lane), Utils.format(
          "Lane '{}' does not exist", lane));
      fullPayload.get(to).addAll(records);
    }
  }

}
