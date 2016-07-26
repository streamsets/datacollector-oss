/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.runner;

import com.streamsets.pipeline.api.Record;

import java.util.List;
import java.util.Map;

public interface PipeBatch {
  int getBatchSize();

  String getPreviousOffset();

  void setNewOffset(String offset);

  BatchImpl getBatch(Pipe pipe);

  BatchMakerImpl startStage(StagePipe pipe);

  void completeStage(BatchMakerImpl batchMaker, EventSink eventSink);

  void commitOffset();

  Map<String, List<Record>> getLaneOutputRecords(List<String> pipeLanes);

  void overrideStageOutput(StagePipe pipe, StageOutput stageOutput);

  List<StageOutput> getSnapshotsOfAllStagesOutput();

  ErrorSink getErrorSink();

  void moveLane(String inputLane, String outputLane);

  void moveLaneCopying(String inputLane, List<String> outputLanes);

  void combineLanes(List<String> lanes, String to);

  int getInputRecords();

  int getOutputRecords();

  int getErrorRecords();

  int getErrorMessages();

}
