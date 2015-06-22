/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution;

import java.util.List;

// one per SDC
public interface SnapshotStore extends PipelineListener {

  // it implements PipelineListener, so when a pipeline starts/stops it can prepare/flush any data related to that
  // pipeline the store may need/have

  // creates a snapshot info, in progress
  public SnapshotInfo create(String user);

  // saves the data of the snapshot and updates the corresponding snapshot info.
  public void save(String id, Snapshot snapshot);

  // retrieves a snapshot base on its ID
  public Snapshot get(String id);

  // lists all available snapshots for a pipeline
  public List<SnapshotInfo> getSummaryForPipeline(String name, String rev);

  // deletes a snapshot
  public void deleteSnapshot(String id);

}
