/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner;

import java.util.List;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.dataCollector.execution.PipelineState;
import com.streamsets.dataCollector.execution.PipelineStatus;
import com.streamsets.dataCollector.execution.Runner;
import com.streamsets.dataCollector.execution.Snapshot;
import com.streamsets.dataCollector.execution.SnapshotInfo;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;

public class ClusterRunner implements Runner {

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getRev() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getUser() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void resetOffset() {
    // TODO Auto-generated method stub

  }

  @Override
  public PipelineStatus getStatus() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void onDataCollectorStart() {
    // TODO Auto-generated method stub

  }

  @Override
  public void onDataCollectorStop() {
    // TODO Auto-generated method stub

  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub

  }

  @Override
  public void start() {
    // TODO Auto-generated method stub

  }

  @Override
  public String captureSnapshot(String name, int batches) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Snapshot getSnapshot(String id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SnapshotInfo> getSnapshotsInfo() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteSnapshot(String id) {
    // TODO Auto-generated method stub

  }

  @Override
  public List<PipelineState> getHistory() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public MetricRegistry getMetric() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Record> getErrorRecords(String stage, int max) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ErrorMessage> getErrorMessages(String stage, int max) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Record> getSampledRecords(String sampleId, int max) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteAlert(String alertId) {
    // TODO Auto-generated method stub

  }

}
