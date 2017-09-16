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
package com.streamsets.datacollector.runner.preview;

import com.streamsets.datacollector.runner.BatchImpl;
import com.streamsets.datacollector.runner.BatchMakerImpl;
import com.streamsets.datacollector.runner.ErrorSink;
import com.streamsets.datacollector.runner.EventSink;
import com.streamsets.datacollector.runner.Pipe;
import com.streamsets.datacollector.runner.PipeBatch;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.runner.StagePipe;
import com.streamsets.pipeline.api.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StagePreviewPipeBatch implements PipeBatch {
  private final String instanceName;
  private final List<Record> inputRecords;
  private final List<StageOutput> stageOutputSnapshot;
  private final ErrorSink errorSink;
  private final EventSink eventSink;

  public StagePreviewPipeBatch(String instanceName, List<Record> inputRecords) {
    this.instanceName = instanceName;
    this.inputRecords = inputRecords;
    stageOutputSnapshot = new ArrayList<>();
    this.errorSink = new ErrorSink();
    this.eventSink = new EventSink();
  }

  @Override
  public int getBatchSize() {
    return -1;
  }

  @Override
  public String getPreviousOffset() {
    return null;
  }

  @Override
  public void setNewOffset(String offset) {
    throw new UnsupportedOperationException("setNewOffset()");
  }

  @Override
  public BatchImpl getBatch(Pipe pipe) {
    return new BatchImpl(instanceName, null, null, inputRecords);
  }

  @Override
  public void skipStage(Pipe pipe) {
  }

  @Override
  public BatchMakerImpl startStage(StagePipe pipe) {
    return new BatchMakerImpl(pipe, true);
  }

  @Override
  public void completeStage(BatchMakerImpl batchMaker) {
    stageOutputSnapshot.add(new StageOutput(instanceName, batchMaker.getStageOutputSnapshot(), errorSink, eventSink));
  }

  @Override
  public void completeStage(StagePipe pipe) {
  }

  @Override
  public Map<String, List<Record>> getLaneOutputRecords(List<String> pipeLanes) {
    throw new UnsupportedOperationException("getLaneOutputRecords()");
  }


  @Override
  public void overrideStageOutput(StagePipe pipe, StageOutput stageOutput) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<StageOutput> getSnapshotsOfAllStagesOutput() {
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
    throw new UnsupportedOperationException("moveLane()");
  }

  @Override
  public void moveLaneCopying(String inputLane, List<String> outputLanes) {
    throw new UnsupportedOperationException("moveLaneCopying()");
  }

  @Override
  public int getInputRecords() {
    return 0;
  }

  @Override
  public int getOutputRecords() {
    return 0;
  }

  @Override
  public int getErrorRecords() {
    return 0;
  }

  @Override
  public int getErrorMessages() {
    return 0;
  }

  @Override
  public List<StageOutput> createFailureSnapshot() {
    throw new UnsupportedOperationException("createFailureSnapshot()");
  }
}
