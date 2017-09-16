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

import com.streamsets.pipeline.api.Record;

import java.util.List;
import java.util.Map;

public interface PipeBatch {
  int getBatchSize();

  String getPreviousOffset();

  void setNewOffset(String offset);

  BatchImpl getBatch(Pipe pipe);

  /**
   * During destroy() phase, rather then running this stage, simply skip it (but still propagate empty output).
   */
  void skipStage(Pipe pipe);

  /**
   * Start processing stage
   */
  BatchMakerImpl startStage(StagePipe pipe);

  /**
   * Complete stage on normal execution (while pipeline is running)
   */
  void completeStage(BatchMakerImpl batchMaker);

  /**
   * Complete stage on during destroy() phase.
   */
  void completeStage(StagePipe pipe);

  Map<String, List<Record>> getLaneOutputRecords(List<String> pipeLanes);

  void overrideStageOutput(StagePipe pipe, StageOutput stageOutput);

  List<StageOutput> getSnapshotsOfAllStagesOutput();

  ErrorSink getErrorSink();

  EventSink getEventSink();

  void moveLane(String inputLane, String outputLane);

  void moveLaneCopying(String inputLane, List<String> outputLanes);

  int getInputRecords();

  int getOutputRecords();

  int getErrorRecords();

  int getErrorMessages();

  /**
   * Create partial snapshot from in-memory structures. This method will be called only on pipeline failure.
   */
  List<StageOutput> createFailureSnapshot();
}
