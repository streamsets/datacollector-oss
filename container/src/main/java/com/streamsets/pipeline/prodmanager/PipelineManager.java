/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.production.ProductionPipeline;
import com.streamsets.pipeline.snapshotstore.SnapshotInfo;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.task.Task;

import java.io.InputStream;
import java.util.List;

public interface PipelineManager extends Task {

  ProductionPipeline getProductionPipeline();

  PipelineState getPipelineState();

  void addStateEventListener(StateEventListener stateListener);

  void removeStateEventListener(StateEventListener stateListener);

  void addAlertEventListener(AlertEventListener alertEventListener);

  void removeAlertEventListener(AlertEventListener alertEventListener);

  void addMetricsEventListener(MetricsEventListener metricsEventListener);

  void removeMetricsEventListener(MetricsEventListener metricsEventListener);

  void broadcastAlerts(RuleDefinition ruleDefinition);

  void resetOffset(String pipelineName, String rev) throws PipelineManagerException;

  List<SnapshotInfo> getSnapshotsInfo() throws PipelineStoreException;

  void captureSnapshot(String snapshotName, int batchSize) throws PipelineManagerException;

  SnapshotStatus getSnapshotStatus(String snapshotName);

  InputStream getSnapshot(String pipelineName, String rev, String snapshotName) throws PipelineManagerException;

  List<Record> getErrorRecords(String instanceName, int size) throws PipelineManagerException;

  List<Record> getSampledRecords(String sampleDefinitionId, int size) throws PipelineManagerException;

  List<ErrorMessage> getErrorMessages(String instanceName, int size) throws PipelineManagerException;

  List<PipelineState> getHistory(String pipelineName, String rev, boolean fromBeginning)
    throws PipelineManagerException;

  void deleteSnapshot(String pipelineName, String rev, String snapshotName);

  PipelineState startPipeline(String name, String rev) throws PipelineStoreException
        , PipelineManagerException, PipelineRuntimeException, StageException;

  PipelineState stopPipeline(boolean nodeProcessShutdown) throws PipelineManagerException;

  MetricRegistry getMetrics();

  void deleteHistory(String pipelineName, String rev) throws PipelineManagerException;

  boolean deleteAlert(String alertId) throws PipelineManagerException;
}
