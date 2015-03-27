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

  public void storeSnapshot(String pipelineName, String rev, String snapshotName, List<StageOutput> snapshot);

  public List<StageOutput> retrieveSnapshot(String pipelineName, String rev, String snapshotName);

  public SnapshotStatus getSnapshotStatus(String pipelineName, String rev, String snapshotName);

  public void deleteSnapshot(String pipelineName, String rev, String snapshotName);

  public InputStream getSnapshot(String pipelineName, String rev, String snapshotName);

  public List<SnapshotInfo> getSnapshots(String pipelineName, String rev);

}
