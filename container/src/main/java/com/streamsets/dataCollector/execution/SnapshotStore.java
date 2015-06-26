/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution;

import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.util.PipelineException;

import java.util.List;

// one per SDC
public interface SnapshotStore {

  // it implements PipelineListener, so when a pipeline starts/stops it can prepare/flush any data related to that
  // pipeline the store may need/have

  // creates a snapshot info, in progress
  public SnapshotInfo create(String user, String name, String rev, String id) throws PipelineException;;

  // saves the data of the snapshot and updates the corresponding snapshot info.
  public void save(String name, String rev, String id, List<List<StageOutput>> snapshotBatches) throws PipelineException;

  // retrieves a snapshot base on its ID
  // The caller must close the stream by calling close() on the Snapshot
  public Snapshot get(String name, String rev, String id) throws PipelineException;

  // lists all available snapshots for a pipeline
  public List<SnapshotInfo> getSummaryForPipeline(String name, String rev) throws PipelineException;;

  // deletes a snapshot
  public void deleteSnapshot(String name, String rev, String id) throws PipelineException;;

}
