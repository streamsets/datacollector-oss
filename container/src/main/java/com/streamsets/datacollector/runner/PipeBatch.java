/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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

  void completeStage(BatchMakerImpl batchMaker);

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
