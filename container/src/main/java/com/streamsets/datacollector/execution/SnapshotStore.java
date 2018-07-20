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
package com.streamsets.datacollector.execution;

import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.util.PipelineException;

import java.util.List;

// one per SDC
public interface SnapshotStore {

  // it implements PipelineListener, so when a pipeline starts/stops it can prepare/flush any data related to that
  // pipeline the store may need/have

  // creates a snapshot info, in progress
  public SnapshotInfo create(String user, String name, String rev, String id, String label, boolean failureSnapshot) throws PipelineException;

  // saves the data of the snapshot and updates the corresponding snapshot info.
  public SnapshotInfo save(
      String name,
      String rev,
      String id,
      long batchNumber,
      List<List<StageOutput>> snapshotBatches
  ) throws PipelineException;

  // Updates the label of snapshot by updating the Snapshot Info
  public SnapshotInfo updateLabel(String name, String rev, String id, String snapshotLabel) throws PipelineException;

  // retrieves a snapshot base on its ID
  // The caller must close the stream by calling close() on the Snapshot
  public Snapshot get(String name, String rev, String id) throws PipelineException;

  public SnapshotInfo getInfo(String name, String rev, String id) throws PipelineException;

  // lists all available snapshots for a pipeline
  public List<SnapshotInfo> getSummaryForPipeline(String name, String rev) throws PipelineException;;

  // deletes a snapshot
  public void deleteSnapshot(String name, String rev, String id) throws PipelineException;;

}
