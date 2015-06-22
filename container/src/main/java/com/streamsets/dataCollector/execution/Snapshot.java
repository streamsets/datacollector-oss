/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution;

import com.streamsets.pipeline.runner.StageOutput;

import java.util.List;

public interface Snapshot {

  SnapshotInfo getInfo();

  public List<List<StageOutput>> getOutput();

}
