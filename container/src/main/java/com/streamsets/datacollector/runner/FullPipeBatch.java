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
package com.streamsets.datacollector.runner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FullPipeBatch implements PipeBatch {

  private final String sourceEntity;
  private final String lastOffset;
  private final int batchSize;
  private final Map<String, List<Record>> fullPayload;
  private final Set<String> processedStages;
  private final List<StageOutput> stageOutputSnapshot;
  private final ErrorSink errorSink;
  private final EventSink eventSink;
  private String newOffset;
  private int inputRecords;
  private int outputRecords;
  private RateLimiter rateLimiter;

  public FullPipeBatch(String sourceEntity, String lastOffset, int batchSize, boolean snapshotStagesOutput) {
    this.sourceEntity = sourceEntity;
    this.lastOffset = lastOffset;
    this.batchSize = batchSize;
    fullPayload = new HashMap<>();
    processedStages = new HashSet<>();
    stageOutputSnapshot = (snapshotStagesOutput) ? new ArrayList<StageOutput>() : null;
    errorSink = new ErrorSink();
    eventSink = new EventSink();
  }

  @VisibleForTesting
  Map<String, List<Record>> getFullPayload() {
    return fullPayload;
  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public String getPreviousOffset() {
    return lastOffset;
  }

  @Override
  public void setNewOffset(String offset) {
    newOffset = offset;
  }

  public String getNewOffset() {
    return newOffset;
  }

  public void setRateLimiter(@Nullable RateLimiter rateLimiter) {
    this.rateLimiter = rateLimiter;
  }

  @Override
  @SuppressWarnings("unchecked")
  public BatchImpl getBatch(final Pipe pipe) {
    List<Record> records = new ArrayList<>();
    List<String> inputLanes = pipe.getInputLanes();
    for (String inputLane : inputLanes) {
      records.addAll(fullPayload.get(inputLane));
    }
    if (pipe.getStage().getDefinition().getType().isOneOf(StageType.TARGET, StageType.EXECUTOR)) {
      outputRecords += records.size();
    }
    return new BatchImpl(pipe.getStage().getInfo().getInstanceName(), sourceEntity, lastOffset, records);
  }

  @Override
  public BatchMakerImpl startStage(StagePipe pipe) {
    String stageName = pipe.getStage().getInfo().getInstanceName();
    Preconditions.checkState(!processedStages.contains(stageName), Utils.formatL(
      "The stage '{}' has been processed already", stageName));
    processedStages.add(stageName);
    for (String output : pipe.getOutputLanes()) {
      fullPayload.put(output, null);
    }
    int recordAllowance = (pipe.getStage().getDefinition().getType() == StageType.SOURCE)
                          ? getBatchSize() : Integer.MAX_VALUE;
    BatchMakerImpl batchMaker = new BatchMakerImpl(pipe, stageOutputSnapshot != null, recordAllowance);
    batchMaker.setRateLimiter(rateLimiter);
    return batchMaker;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void skipStage(Pipe pipe) {
    // Fill expected stage output lanes with empty lists
    pipe.getOutputLanes().stream().forEach(lane -> fullPayload.put((String)lane, Collections.emptyList()));
    // Components are allowed to generate events on destroy phase and hence we need to use default empty
    // list only if the event lane was not filled before.
    pipe.getEventLanes().stream().forEach(lane -> fullPayload.putIfAbsent((String)lane, Collections.emptyList()));
  }

  @Override
  public void completeStage(BatchMakerImpl batchMaker) {
    StagePipe pipe = batchMaker.getStagePipe();
    if (pipe.getStage().getDefinition().getType() == StageType.SOURCE) {
      inputRecords += batchMaker.getSize() +
          errorSink.getErrorRecords(pipe.getStage().getInfo().getInstanceName()).size();
    }
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
      stageOutputSnapshot.add(new StageOutput(instanceName, batchMaker.getStageOutputSnapshot(), errorSink, eventSink));
    }
    if (pipe.getStage().getDefinition().getType().isOneOf(StageType.TARGET, StageType.EXECUTOR)) {
      outputRecords -= errorSink.getErrorRecords(pipe.getStage().getInfo().getInstanceName()).size();
    }
    completeStage(pipe);
  }

  @Override
  public void completeStage(StagePipe pipe) {
    List<String> inputLanes = pipe.getInputLanes();
    for(String inputLane : inputLanes) {
      fullPayload.remove(inputLane);
    }

    if(pipe.getEventLanes().size() == 1) {
      fullPayload.put(pipe.getEventLanes().get(0), eventSink.getStageEvents(pipe.getStage().getInfo().getInstanceName()));
    }
  }

  @Override
  public Map<String, List<Record>> getLaneOutputRecords(List<String> pipeLanes) {
    Map<String, List<Record>> snapshot = new HashMap<>();
    for (String pipeLane : pipeLanes) {
      //The observer will copy
      snapshot.put(pipeLane, fullPayload.get(pipeLane));
    }
    return snapshot;
  }

  //TODO rename method
  private List<Record> createSnapshot(List<Record> records) {
    List<Record> list = new ArrayList<>(records.size());
    for (Record record : records) {
      list.add(((RecordImpl) record).clone());
    }
    return list;
  }

  private Map<String, List<Record>> createSnapshot(Map<String, List<Record>> output) {
    Map<String, List<Record>> copy = new HashMap<>();
    for (Map.Entry<String, List<Record>> entry : output.entrySet()) {
      copy.put(entry.getKey(), createSnapshot(entry.getValue()));
    }
    return copy;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void overrideStageOutput(StagePipe pipe, StageOutput stageOutput) {
    startStage(pipe);
    for (String pipeLaneName : pipe.getOutputLanes()) {
      String stageLaneName = LaneResolver.removePostFixFromLane(pipeLaneName);
      fullPayload.put(pipeLaneName, stageOutput.getOutput().get(stageLaneName));
    }
    if(pipe.getEventLanes().size() == 1) {
      fullPayload.put(pipe.getEventLanes().get(0), stageOutput.getEventRecords());
    }
    if (stageOutputSnapshot != null) {
      stageOutputSnapshot.add(new StageOutput(
          stageOutput.getInstanceName(),
          createSnapshot(stageOutput.getOutput()),
          stageOutput.getErrorRecords(),
          stageOutput.getStageErrors(),
          stageOutput.getEventRecords()
      ));
    }
  }

  @Override
  public List<StageOutput> getSnapshotsOfAllStagesOutput() {
    return stageOutputSnapshot;
  }

  @Override
  public List<StageOutput> createFailureSnapshot() {
    // Stage name -> (Lane name -> Records)
    Map<String, Map<String, List<Record>>> salvagedStageOutputs = new LinkedHashMap<>();

    // Salvage what is in memory
    fullPayload.forEach((lane, records) -> {
      // We can work only with multiplexer lanes as only those encode all the information we need
      if(!lane.endsWith(LaneResolver.MULTIPLEXER_OUT)) {
        return;
      }

      // Split the lane name to get out - stage and lane name
      String[] parts = lane.split(LaneResolver.SEPARATOR);
      Utils.checkState(parts.length == 3, "Invalid multiplexer lane name: " + lane);
      String stageName = parts[1];

      parts = parts[0].split(LaneResolver.ROUTING_SEPARATOR);
      Utils.checkState(parts.length == 2, "Invalid multiplexer lane name: " + lane);
      String laneName = parts[0];

      Map<String, List<Record>> stageOutput = salvagedStageOutputs.computeIfAbsent(stageName, name -> new LinkedHashMap<>());

      // Since we can copy records for lanes that diverge, we use putIfAbsent()
      stageOutput.putIfAbsent(laneName, records);
    });

    // Convert salvaged structure to final snapshot
    List<StageOutput> stageOutputSnapshot = new ArrayList<>();
    salvagedStageOutputs.forEach((stageName, outputs) -> {
      stageOutputSnapshot.add(new StageOutput(stageName, outputs, errorSink, eventSink));
    });

    return stageOutputSnapshot;
  }

  @Override
  public ErrorSink getErrorSink() {
    return errorSink;
  }

  @Override
  public EventSink getEventSink() {
    return eventSink;
  }


  @Override
  public void moveLane(String inputLane, String outputLane) {
    fullPayload.put(outputLane, Preconditions.checkNotNull(fullPayload.remove(inputLane), Utils.formatL(
        "Stream '{}' does not exist", inputLane)));
  }

  @Override
  public void moveLaneCopying(String inputLane, List<String> outputLanes) {
    List<Record> records = Preconditions.checkNotNull(fullPayload.remove(inputLane), Utils.formatL(
        "Stream '{}' does not exist", inputLane));
    for (String lane : outputLanes) {
      Preconditions.checkState(!fullPayload.containsKey(lane), Utils.formatL("Lane '{}' already exists", lane));
      fullPayload.put(lane, createCopy(records));
    }
  }

  private List<Record> createCopy(List<Record> records) {
    List<Record> list = new ArrayList<>(records.size());
    for (Record record : records) {
      list.add(((RecordImpl) record).clone());
    }
    return list;
  }

  private List<String> remove(List<String> from, Collection<String> values) {
    List<String> list = new ArrayList<>(from);
    list.removeAll(values);
    return list;
  }

  @Override
  public int getInputRecords() {
    return inputRecords;
  }

  @Override
  public int getOutputRecords() {
    return outputRecords;
  }

  @Override
  public int getErrorRecords() {
    return errorSink.getTotalErrorRecords();
  }

  @Override
  public int getErrorMessages() {
    return errorSink.getTotalErrorMessages();
  }

  @Override
  public String toString() {
    return Utils.format(
      "PipeBatch[previousOffset='{}' currentOffset='{}' batchSize='{}' keepSnapshot='{}' errorRecords='{}]'",
      lastOffset,
      newOffset,
      batchSize,
      stageOutputSnapshot != null,
      errorSink.size()
    );
  }

}
