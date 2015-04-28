/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Files;
import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.cluster.ApplicationState;
import com.streamsets.pipeline.cluster.SparkManager;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.PipelineDefConfigs;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.production.ProductionObserver;
import com.streamsets.pipeline.runner.production.ProductionPipeline;
import com.streamsets.pipeline.runner.production.ProductionPipelineBuilder;
import com.streamsets.pipeline.runner.production.ProductionPipelineRunner;
import com.streamsets.pipeline.runner.production.ProductionSourceOffsetTracker;
import com.streamsets.pipeline.snapshotstore.SnapshotInfo;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import com.streamsets.pipeline.validation.ValidationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class ClusterPipelineManager extends AbstractTask implements PipelineManager {
  private static final Logger LOG = LoggerFactory.getLogger(StandalonePipelineManagerTask.class);
  private static final String APPLICATION_STATE = "cluster.application.state";
  private final RuntimeInfo runtimeInfo;
  private final Configuration configuration;
  private final PipelineStoreTask pipelineStore;
  private final StageLibraryTask stageLibrary;
  private final StateTracker stateTracker;
  private final SparkManager sparkManager;

  public ClusterPipelineManager(RuntimeInfo runtimeInfo, Configuration configuration, PipelineStoreTask pipelineStore,
      StageLibraryTask stageLibrary) {
    super(ClusterPipelineManager.class.getSimpleName());
    this.runtimeInfo = runtimeInfo;
    this.configuration = configuration;
    this.pipelineStore = pipelineStore;
    this.stageLibrary = stageLibrary;
    stateTracker = new StateTracker(runtimeInfo, configuration);
    sparkManager = new SparkManager(Files.createTempDir());
  }

  @Override
  protected void initTask() {
    PipelineState ps = getPipelineState();
    if(ps != null && ps.getState() == State.RUNNING) {
      try {
        Map<String, Object> attributes = new HashMap<>();
        attributes.putAll(ps.getAttributes());
        ApplicationState appState = (ApplicationState)attributes.get(APPLICATION_STATE);
        if (appState == null) {
          // do nothing
        } else if(sparkManager.isRunning(appState)) {
          stateTracker.setState(ps.getName(), ps.getRev(), State.RUNNING, null, null, attributes);
        } else {
          stateTracker.setState(ps.getName(), ps.getRev(), State.ERROR, "Cluster Pipeline not running anymore", null,
            attributes);
        }
      } catch (Exception ex) {
        throw new RuntimeException("Error starting pipeline: " + ex, ex);
      }
    }
  }

  @Override
  protected void stopTask() {
    // DO nothing, SDC goes down but pipeline should continue running
  }

  @Override
  public PipelineState getPipelineState() {
    return stateTracker.getState();
  }

  @Override
  public void addStateEventListener(StateEventListener stateListener) {
    stateTracker.addStateEventListener(stateListener);
  }

  @Override
  public void removeStateEventListener(StateEventListener stateListener) {
    stateTracker.removeStateEventListener(stateListener);
  }

  @Override
  public void addAlertEventListener(AlertEventListener alertEventListener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeAlertEventListener(AlertEventListener alertEventListener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addMetricsEventListener(MetricsEventListener metricsEventListener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeMetricsEventListener(MetricsEventListener metricsEventListener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void broadcastAlerts(RuleDefinition ruleDefinition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resetOffset(String pipelineName, String rev) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SnapshotInfo> getSnapshotsInfo() throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void captureSnapshot(String snapshotName, int batchSize) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SnapshotStatus getSnapshotStatus(String snapshotName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getSnapshot(String pipelineName, String rev, String snapshotName) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Record> getErrorRecords(String instanceName, int size) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Record> getSampledRecords(String sampleDefinitionId, int size) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ErrorMessage> getErrorMessages(String instanceName, int size) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PipelineState> getHistory(String pipelineName, String rev, boolean fromBeginning) throws
      PipelineManagerException {
    if(!pipelineStore.hasPipeline(pipelineName)) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0109, pipelineName);
    }
    return stateTracker.getHistory(pipelineName, rev, fromBeginning);
  }

  @Override
  public void deleteSnapshot(String pipelineName, String rev, String snapshotName) {
    throw new UnsupportedOperationException();
  }

  public void validateStateTransition(String pipelineName, String rev, State toState) throws PipelineManagerException {
    validatePipelineExistence(pipelineName);
    PipelineState ps = getPipelineState();
    if (ps != null) {
      checkState(StandalonePipelineManagerTask.VALID_TRANSITIONS.get(ps.getState()).contains(toState),
        ContainerError.CONTAINER_0102, ps.getState(), toState);
    }
  }

  private void checkState(boolean expr, ContainerError error, Object... args)
    throws PipelineManagerException {
    if(!expr) {
      throw new PipelineManagerException(error, args);
    }
  }

  private void validatePipelineExistence(String pipelineName) throws PipelineManagerException {
    if(!pipelineStore.hasPipeline(pipelineName)) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0109, pipelineName);
    }
  }

  @Override
  public PipelineState startPipeline(String name, String rev) throws PipelineStoreException, PipelineManagerException,
      PipelineRuntimeException, StageException {
    validateStateTransition(name, rev, State.RUNNING);
    PipelineConfiguration pipelineConf = pipelineStore.load(name, rev);
    ExecutionMode executionMode = ExecutionMode.valueOf((String) pipelineConf.getConfiguration(
        PipelineDefConfigs.EXECUTION_MODE_CONFIG).getValue());
    if (executionMode == ExecutionMode.CLUSTER) {
      Map<String, String> environment = new HashMap<>();
      Map<String, String> sourceInfo = new HashMap<>();
      sparkManager.submit(pipelineConf, stageLibrary, new File(runtimeInfo.getConfigDir()),
        environment, sourceInfo, 180);

      //

      Map<String, Object> attributes = new HashMap<>();
      stateTracker.setState(name, rev, State.RUNNING, "Starting cluster pipeline", null, attributes);
      return stateTracker.getState();
    } else {
      throw new PipelineManagerException(ValidationError.VALIDATION_0073);
    }
  }

  @Override
  public PipelineState stopPipeline(boolean nodeProcessShutdown) throws PipelineManagerException {
    PipelineState state = stateTracker.getState();
    if (!nodeProcessShutdown) {
      // TODO validate state transition
      ApplicationState appState = (ApplicationState)state.getAttributes().get(APPLICATION_STATE);
      // TODO throw error if null
      if (appState != null) {
        try {
          sparkManager.kill(appState);
        } catch (TimeoutException ex) {
          throw new PipelineManagerException(ValidationError.VALIDATION_0073);
        }
      }
      stateTracker.setState(state.getName(), getName(), State.STOPPED, "Stopping cluster pipeline", null,
                            state.getAttributes());
      return stateTracker.getState();
    }
    return state;
  }

  @Override
  public MetricRegistry getMetrics() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteHistory(String pipelineName, String rev) throws PipelineManagerException {
    LOG.debug("Deleting history for pipeline {}", pipelineName);
    PipelineState state = getPipelineState();
    if(state != null && state.getName().equals(pipelineName) && state.getState() == State.RUNNING) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0111, pipelineName);
    }
    stateTracker.deleteHistory(pipelineName, rev);
  }

  @Override
  public boolean deleteAlert(String alertId) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ProductionPipeline getProductionPipeline() {
    throw new UnsupportedOperationException();
  }

  @VisibleForTesting
  ProductionPipeline createProductionPipeline(String name, String rev,
                                              com.streamsets.pipeline.util.Configuration configuration,
                                              StageLibraryTask stageLibrary, ProductionObserver observer,
                                              PipelineConfiguration pipelineConfiguration,
                                              BlockingQueue<Object> observeRequests)
    throws PipelineStoreException, PipelineRuntimeException, StageException, PipelineManagerException {

    DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
    for(ConfigConfiguration config : pipelineConfiguration.getConfiguration()) {
      if(Constants.DELIVERY_GUARANTEE.equals(config.getName())) {
        deliveryGuarantee = DeliveryGuarantee.valueOf((String)config.getValue());
      }
    }
    //create the pipeline directory eagerly.
    //This helps avoid race conditions when different stores attempt to create directories
    //Creating directory eagerly also avoids the need of synchronization
    createPipelineDirIfNotExist(name);
    stateTracker.register(name, rev);

    ProductionSourceOffsetTracker offsetTracker = new ProductionSourceOffsetTracker(name, rev, runtimeInfo);
    ProductionPipelineRunner runner = new ProductionPipelineRunner(runtimeInfo, null,
      deliveryGuarantee, name, rev, observeRequests, configuration, StandalonePipelineManagerTask.
      getMemoryLimitConfiguration(pipelineConfiguration));

    ProductionPipelineBuilder builder = new ProductionPipelineBuilder(stageLibrary, name, rev, runtimeInfo,
      pipelineConfiguration);
    return builder.build(runner, offsetTracker, observer);
  }

  private void createPipelineDirIfNotExist(String name) throws PipelineManagerException {
    File pipelineDir = new File(new File(runtimeInfo.getDataDir(), StandalonePipelineManagerTask.RUN_INFO_DIR),
      PipelineDirectoryUtil.getEscapedPipelineName(name));
    if(!pipelineDir.exists()) {
      if(!pipelineDir.mkdirs()) {
        throw new PipelineManagerException(ContainerError.CONTAINER_0110, name,
          Utils.format("'{}' mkdir failed", pipelineDir));
      }
    }
  }
}
