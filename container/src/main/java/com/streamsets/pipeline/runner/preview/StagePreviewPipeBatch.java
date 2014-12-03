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
package com.streamsets.pipeline.runner.preview;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.runner.BatchImpl;
import com.streamsets.pipeline.runner.BatchMakerImpl;
import com.streamsets.pipeline.runner.ErrorSink;
import com.streamsets.pipeline.runner.Pipe;
import com.streamsets.pipeline.runner.PipeBatch;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.runner.StagePipe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StagePreviewPipeBatch implements PipeBatch {
  private final String instanceName;
  private final List<Record> inputRecords;
  private final List<StageOutput> stageOutputSnapshot;
  private final ErrorSink errorSink;

  public StagePreviewPipeBatch(String instanceName, List<Record> inputRecords) {
    this.instanceName = instanceName;
    this.inputRecords = inputRecords;
    stageOutputSnapshot = new ArrayList<StageOutput>();
    this.errorSink = new ErrorSink();
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
    return new BatchImpl(instanceName, null, inputRecords);
  }

  @Override
  public BatchMakerImpl startStage(StagePipe pipe) {
    return new BatchMakerImpl(pipe, true);
  }

  @Override
  public void completeStage(BatchMakerImpl batchMaker) {
    StagePipe pipe = batchMaker.getStagePipe();
    Map<String, List<Record>> stageOutput = batchMaker.getStageOutput();
    // convert lane names from stage naming to pipe naming when adding to the payload
    // leveraging the fact that the stage output lanes and the pipe output lanes are in the same order
    List<String> stageLaneNames = pipe.getStage().getConfiguration().getOutputLanes();
    stageOutputSnapshot.add(new StageOutput(instanceName, batchMaker.getStageOutputSnapshot(),
                                            errorSink.getErrorRecords(instanceName)));
  }

  @Override
  public void commitOffset() {
    throw new UnsupportedOperationException("commitOffset()");
  }

  @Override
  public Map<String, List<Record>> getPipeLanesSnapshot(List<String> pipeLanes) {
    throw new UnsupportedOperationException("getPipeLanesSnapshot()");
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
  public void moveLane(String inputLane, String outputLane) {
    throw new UnsupportedOperationException("moveLane()");
  }

  @Override
  public void moveLaneCopying(String inputLane, List<String> outputLanes) {
    throw new UnsupportedOperationException("moveLaneCopying()");
  }

  @Override
  public void combineLanes(List<String> lanes, String to) {
    throw new UnsupportedOperationException("combineLanes()");
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
}
