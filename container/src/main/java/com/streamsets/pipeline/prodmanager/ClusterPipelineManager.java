/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.callback.CallbackInfo;
import com.streamsets.pipeline.cluster.ApplicationState;
import com.streamsets.pipeline.cluster.SparkManager;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.PipelineDefConfigs;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.production.ProductionPipeline;
import com.streamsets.pipeline.snapshotstore.SnapshotInfo;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.PipelineConfigurationUtil;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import com.streamsets.pipeline.validation.ValidationError;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ClusterPipelineManager extends AbstractTask implements PipelineManager {
  private static final Logger LOG = LoggerFactory.getLogger(StandalonePipelineManagerTask.class);
  static final String APPLICATION_STATE = "cluster.application.state";

  static final Map<State, Set<State>> VALID_TRANSITIONS = new ImmutableMap.Builder<State, Set<State>>()
    .put(State.STOPPED, ImmutableSet.of(State.RUNNING))
    .put(State.RUNNING, ImmutableSet.of(State.STOPPING, State.ERROR))
    .put(State.STOPPING, ImmutableSet.of(State.STOPPING /*Try stopping many times, this should be no-op*/
      , State.STOPPED))
    .put(State.ERROR, ImmutableSet.of(State.RUNNING, State.STOPPED))
    .build();

  private final RuntimeInfo runtimeInfo;
  private final Configuration configuration;
  private final PipelineStoreTask pipelineStore;
  private final StageLibraryTask stageLibrary;
  private final StateTracker stateTracker;
  private final File tempDir;
  private SparkManager sparkManager;
  private Cache<String, CallbackInfo> slaveCallbackList;

  public ClusterPipelineManager(RuntimeInfo runtimeInfo, Configuration configuration, PipelineStoreTask pipelineStore,
                                StageLibraryTask stageLibrary) {
    this(runtimeInfo, configuration, pipelineStore, stageLibrary, null,
      new StateTracker(runtimeInfo, configuration));
  }

  @VisibleForTesting
  ClusterPipelineManager(RuntimeInfo runtimeInfo, Configuration configuration, PipelineStoreTask pipelineStore,
      StageLibraryTask stageLibrary, SparkManager sparkManager, StateTracker stateTracker) {
    super(ClusterPipelineManager.class.getSimpleName());
    this.runtimeInfo = runtimeInfo;
    this.configuration = configuration;
    this.pipelineStore = pipelineStore;
    this.stageLibrary = stageLibrary;
    this.stateTracker = stateTracker;
    this.tempDir = Files.createTempDir();
    this.sparkManager = sparkManager;
    if (this.sparkManager == null) {
      this.sparkManager = new SparkManager(tempDir);
    }
    slaveCallbackList = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build();
  }

  @Override
  protected void initTask() {
    final PipelineState ps = getPipelineState();
    if(ps != null && ps.getState() == State.RUNNING) {
      final Map<String, Object> attributes = new HashMap<>();
      attributes.putAll(ps.getAttributes());
      ApplicationState appState = (ApplicationState)attributes.get(APPLICATION_STATE);
      if (appState == null) {
        LOG.error(ContainerError.CONTAINER_0108.getMessage(), "pipeline is running but application state does not " +
          "exist");
      } else {
        Futures.addCallback(sparkManager.isRunning(appState), new FutureCallback<Boolean>() {
          @Override
          public void onSuccess(Boolean running) {
            if (!running) {
              transitionToError("Cluster Pipeline not running anymore");
            }
          }

          private void transitionToError(String msg) {
            try {
              validateStateTransition(ps.getName(), ps.getRev(), State.ERROR);
              attributes.remove(APPLICATION_STATE);
              stateTracker.setState(ps.getName(), ps.getRev(), State.ERROR, msg, null, attributes);
            } catch (Exception e) {
              LOG.error("An exception occurred while committing ERROR state: {}", e, e);
            }
          }
          @Override
          public void onFailure(Throwable throwable) {
            String msg = Utils.format("An error occurred while checking status: {}", throwable);
            LOG.error(msg, throwable);
            transitionToError(msg);
          }
        });
      }
    }
  }

  @Override
  protected void stopTask() {
    // DO nothing, SDC goes down but pipeline should continue running
    FileUtils.deleteQuietly(tempDir);
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

  @VisibleForTesting
  void validateStateTransition(String pipelineName, String rev, State toState) throws PipelineManagerException {
    validatePipelineExistence(pipelineName);
    PipelineState ps = getPipelineState();
    if (ps != null) {
      checkState(VALID_TRANSITIONS.get(ps.getState()).contains(toState),
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
  public PipelineState startPipeline(final String name, final String rev) throws PipelineStoreException,
    PipelineManagerException, PipelineRuntimeException, StageException {
    validateStateTransition(name, rev, State.RUNNING);
    runtimeInfo.reloadClusterToken();
    PipelineConfiguration pipelineConf = pipelineStore.load(name, rev);
    ExecutionMode executionMode = ExecutionMode.valueOf((String) pipelineConf.getConfiguration(
      PipelineDefConfigs.EXECUTION_MODE_CONFIG).getValue());
    if (executionMode == ExecutionMode.CLUSTER) {
      //create the pipeline directory eagerly.
      //This helps avoid race conditions when different stores attempt to create directories
      //Creating directory eagerly also avoids the need of synchronization
      createPipelineDirIfNotExist(name);
      stateTracker.register(name, rev);
      Map<String, String> environment = new HashMap<>();
      Map<String, String> envConfigMap = PipelineConfigurationUtil.getFlattenedStringMap(PipelineDefConfigs.
        CLUSTER_LAUNCHER_ENV_CONFIG, pipelineConf);
      environment.putAll(envConfigMap);
      Map<String, String> sourceInfo = new HashMap<>();
      File bootstrapDir = new File(System.getProperty("user.dir"),
        "libexec/bootstrap-libs/");
      ListenableFuture submitFuture = sparkManager.submit(pipelineConf, stageLibrary,
        new File(runtimeInfo.getConfigDir()), new File(runtimeInfo.getStaticWebDir()), bootstrapDir, environment,
        sourceInfo);
      // set state of running before adding callback which modified attributes
      Map<String, Object> attributes = new HashMap<>();
      PipelineState ps = stateTracker.getState();
      if (ps != null) {
        attributes.putAll(ps.getAttributes());
      }
      stateTracker.setState(name, rev, State.RUNNING, "Starting cluster pipeline", null, attributes);
      // handle result and update attributes
      Futures.addCallback(submitFuture, new FutureCallback<ApplicationState>() {
        @Override
        public void onSuccess(ApplicationState applicationState) {
          Map<String, Object> attributes = new HashMap<>();
          PipelineState ps = stateTracker.getState();
          if (ps != null) {
            attributes.putAll(ps.getAttributes());
          }
          attributes.put(APPLICATION_STATE, applicationState);
          try {
            stateTracker.setState(name, rev, State.RUNNING, "Starting cluster pipeline", null, attributes);
          } catch (Exception ex) {
            LOG.error("An exception occurred while committing the state: {}", ex, ex);
          }
        }
        @Override
        public void onFailure(Throwable throwable) {
          LOG.error("An error occurred while submitting pipeline: {}", throwable, throwable);
          try {
            validateStateTransition(name, rev, State.ERROR);
            stateTracker.setState(name, rev, State.ERROR, "Error starting cluster: " + throwable, null, null);
          } catch (Exception ex) {
            LOG.error("An exception occurred while committing the state: {}", ex, ex);
          }
        }
      });
      return stateTracker.getState();
    } else {
      throw new PipelineManagerException(ValidationError.VALIDATION_0073);
    }
  }

  @Override
  public PipelineState stopPipeline(boolean nodeProcessShutdown) throws PipelineManagerException {
    final PipelineState ps = stateTracker.getState();
    if (!nodeProcessShutdown) {
      if (ps == null) {
        throw new PipelineManagerException(ContainerError.CONTAINER_0101, "for pipeline");
      } else {
        switch (ps.getState()) {
          case STOPPED:
          case STOPPING:
            // do nothing
            break;
          case RUNNING:
            ApplicationState appState = (ApplicationState) ps.getAttributes().get(APPLICATION_STATE);
            if (appState == null) {
              throw new PipelineManagerException(ContainerError.CONTAINER_0101, "for cluster application");
            } else {
              validateStateTransition(ps.getName(), ps.getRev(), State.STOPPING);
              stateTracker.setState(ps.getName(), ps.getRev(), State.STOPPING, "Stopping cluster pipeline", null,
                ps.getAttributes());
              ListenableFuture<Void> killFuture = sparkManager.kill(appState);
              Futures.addCallback(killFuture, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void ignored) {
                  try {
                    validateStateTransition(ps.getName(), ps.getRev(), State.STOPPED);
                    stateTracker.setState(ps.getName(), ps.getRev(), State.STOPPED, "Stopped cluster pipeline", null,
                      null);
                  } catch (Exception ex) {
                    LOG.error("An exception occurred while committing the state: {}", ex, ex);
                  }
                }
                @Override
                public void onFailure(Throwable throwable) {
                  try {
                    validateStateTransition(ps.getName(), ps.getRev(), State.ERROR);
                    stateTracker.setState(ps.getName(), ps.getRev(), State.ERROR, "Error stopping cluster: " + throwable,
                      null, null);
                  } catch (Exception ex) {
                    LOG.error("An exception occurred while committing the state: {}", ex, ex);
                  }
                }
              });
            }
            break;
          default:
            throw new PipelineManagerException(ContainerError.CONTAINER_0102, ps.getState(), State.STOPPED);

        }
      }
    }
    return stateTracker.getState();
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

  @Override
  public void updateSlaveCallbackInfo(CallbackInfo callbackInfo) {
    if(runtimeInfo.getClusterToken().equals(callbackInfo.getSdcClusterToken())) {
      slaveCallbackList.put(callbackInfo.getSdcURL(), callbackInfo);
    } else {
      throw new RuntimeException("SDC Cluster token not matched");
    }
  }

  @Override
  public Collection<CallbackInfo> getSlaveCallbackList() {
    return slaveCallbackList.asMap().values();
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
