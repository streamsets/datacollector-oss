/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.snapshotstore;

import com.streamsets.pipeline.runner.StageOutput;

import java.io.InputStream;
import java.util.List;

public interface SnapshotStore {

  void storeSnapshot(String pipelineName, List<StageOutput> snapshot);

  List<StageOutput> retrieveSnapshot(String pipelineName);

  SnapshotStatus getSnapshotStatus(String pipelineName);

  void deleteSnapshot(String pipelineName);

  InputStream getSnapshot(String pipelineName);
}
