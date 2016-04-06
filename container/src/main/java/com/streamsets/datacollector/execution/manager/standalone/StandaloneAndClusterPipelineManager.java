/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.execution.manager.standalone;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.PreviewerListener;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.manager.PipelineManagerException;
import com.streamsets.datacollector.execution.manager.PreviewerProvider;
import com.streamsets.datacollector.execution.manager.RunnerProvider;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.validation.ValidationError;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.dc.execution.manager.standalone.ResourceManager;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

import dagger.ObjectGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class StandaloneAndClusterPipelineManager extends AbstractTask implements Manager, PreviewerListener  {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneAndClusterPipelineManager.class);
  private static final String PIPELINE_MANAGER = "PipelineManager";

  private final ObjectGraph objectGraph;

  @Inject RuntimeInfo runtimeInfo;
  @Inject Configuration configuration;
  @Inject PipelineStoreTask pipelineStore;
  @Inject PipelineStateStore pipelineStateStore;
  @Inject StageLibraryTask stageLibrary;
  @Inject @Named("previewExecutor") SafeScheduledExecutorService previewExecutor;
  @Inject @Named("runnerExecutor") SafeScheduledExecutorService runnerExecutor;
  @Inject @Named("managerExecutor") SafeScheduledExecutorService managerExecutor;
  @Inject RunnerProvider runnerProvider;
  @Inject PreviewerProvider previewerProvider;
  @Inject ResourceManager resourceManager;
  @Inject EventListenerManager eventListenerManager;

  private Cache<String, RunnerInfo> runnerCache;
  private Cache<String, Previewer> previewerCache;
  static final long DEFAULT_RUNNER_EXPIRY_INTERVAL = 60*60*1000;
  static final String RUNNER_EXPIRY_INTERVAL = "runner.expiry.interval";
  private final long runnerExpiryInterval;
  private ScheduledFuture<?> runnerExpiryFuture;
  private static final String NAME_AND_REV_SEPARATOR = "::";

  public StandaloneAndClusterPipelineManager(ObjectGraph objectGraph) {
    super(PIPELINE_MANAGER);
    this.objectGraph = objectGraph;
    this.objectGraph.inject(this);
    runnerExpiryInterval = this.configuration.get(RUNNER_EXPIRY_INTERVAL, DEFAULT_RUNNER_EXPIRY_INTERVAL);
    eventListenerManager.addStateEventListener(resourceManager);
    MetricsConfigurator.registerJmxMetrics(runtimeInfo.getMetrics());
  }

  @Override
  public Previewer createPreviewer(String user, String name, String rev) throws PipelineStoreException {
    if (!pipelineStore.hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    }
    Previewer previewer = previewerProvider.createPreviewer(user, name, rev, this, objectGraph);
    previewerCache.put(previewer.getId(), previewer);
    return previewer;
  }

  @Override
  public Previewer getPreviewer(String previewerId) {
    Utils.checkNotNull(previewerId, "previewerId");
    Previewer previewer = previewerCache.getIfPresent(previewerId);
    if (previewer == null) {
      LOG.warn("Cannot find the previewer in cache for id: '{}'", previewerId);
    }
    return previewer;
  }

  @Override
  public Runner getRunner(final String user, final String name, final String rev) throws PipelineStoreException, PipelineManagerException {
    if (!pipelineStore.hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    }
    final String nameAndRevString = getNameAndRevString(name, rev);
    RunnerInfo runnerInfo;
    try {
      runnerInfo = runnerCache.get(nameAndRevString, new Callable<RunnerInfo>() {
        @Override
        public RunnerInfo call() throws PipelineStoreException {
          ExecutionMode executionMode = pipelineStateStore.getState(name, rev).getExecutionMode();
          Runner runner = getRunner(pipelineStateStore.getState(name, rev).getUser(), name, rev, executionMode);
          return new RunnerInfo(runner, executionMode);
        }
      });
      ExecutionMode cachedExecutionMode = runnerInfo.executionMode;
      ExecutionMode persistentExecutionMode = pipelineStateStore.getState(name, rev).getExecutionMode();
      if (cachedExecutionMode == ExecutionMode.CLUSTER) {
        LOG.info("Upgrading execution mode from " + ExecutionMode.CLUSTER + " to " + persistentExecutionMode);
        runnerInfo.executionMode = persistentExecutionMode;
      }
      if (runnerInfo.executionMode != pipelineStateStore.getState(name, rev).getExecutionMode()) {
        LOG.info(Utils.format("Invalidate the existing runner for pipeline '{}::{}' as execution mode has changed",
          name, rev));
        if (!removeRunnerIfActive(runnerInfo.runner)) {
          throw new PipelineManagerException(ValidationError.VALIDATION_0082, pipelineStateStore.getState(name, rev).getExecutionMode(),
            runnerInfo.executionMode);
        } else {
          return getRunner(user, name, rev);
        }
      }
    } catch (ExecutionException ex) {
      if (ex.getCause() instanceof RuntimeException) {
        throw (RuntimeException) ex.getCause();
      } else if (ex.getCause() instanceof PipelineStoreException) {
        throw (PipelineStoreException) ex.getCause();
      } else {
        throw new PipelineStoreException(ContainerError.CONTAINER_0114, ex.toString(), ex);
      }
    }
    return runnerInfo.runner;
  }

  @Override
  public List<PipelineState> getPipelines() throws PipelineStoreException {
    List<PipelineInfo> pipelineInfoList = pipelineStore.getPipelines();
    List<PipelineState> pipelineStateList = new ArrayList<>();
    for (PipelineInfo pipelineInfo : pipelineInfoList) {
      String name = pipelineInfo.getName();
      String rev = pipelineInfo.getLastRev();
      PipelineState pipelineState = pipelineStateStore.getState(name, rev);
      Utils.checkState(pipelineState != null, Utils.format("State for pipeline: '{}::{}' doesn't exist", name, rev));
      pipelineStateList.add(pipelineState);
    }
    return pipelineStateList;
  }

  @Override
  public boolean isPipelineActive(String name, String rev) throws PipelineStoreException {
    if (!pipelineStore.hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    }
    RunnerInfo runnerInfo = runnerCache.getIfPresent(getNameAndRevString(name, rev));
    return (runnerInfo == null) ? false : runnerInfo.runner.getState().getStatus().isActive();
  }

  @Override
  public void initTask() {
    previewerCache = CacheBuilder.newBuilder()
      .expireAfterAccess(30, TimeUnit.MINUTES).removalListener(new RemovalListener<String, Previewer>() {
        @Override
        public void onRemoval(RemovalNotification<String, Previewer> removal) {
          Previewer previewer = removal.getValue();
          LOG.warn("Evicting idle previewer '{}::{}'::'{}' in status '{}'",
            previewer.getName(), previewer.getRev(), previewer.getId(), previewer.getStatus());
        }
      }).build();

    runnerCache = CacheBuilder.newBuilder().build();

    List<PipelineInfo> pipelineInfoList;
    try {
      pipelineInfoList = pipelineStore.getPipelines();
    } catch (PipelineStoreException ex) {
      throw new RuntimeException("Cannot load the list of pipelines from StateStore", ex);
    }
    for (PipelineInfo pipelineInfo : pipelineInfoList) {
      String name = pipelineInfo.getName();
      String rev = pipelineInfo.getLastRev();
      try {
        PipelineState pipelineState = pipelineStateStore.getState(name, rev);
        // Create runner if active
        if (pipelineState.getStatus().isActive()) {
          ExecutionMode executionMode = pipelineState.getExecutionMode();
          Runner runner = getRunner(pipelineState.getUser(), name, rev, executionMode);
          runner.prepareForDataCollectorStart();
          if (runner.getState().getStatus() == PipelineStatus.DISCONNECTED) {
            runnerCache.put(getNameAndRevString(name, rev), new RunnerInfo(runner, executionMode));
            runner.onDataCollectorStart();
          }
        }
      } catch (Exception ex) {
        LOG.error(Utils.format("Error while processing pipeline '{}::{}'", name, rev), ex);
      }
    }

    runnerExpiryFuture = managerExecutor.schedule(new Runnable() {
      @Override
      public void run() {
        for (RunnerInfo runnerInfo : runnerCache.asMap().values()) {
          Runner runner = runnerInfo.runner;
          try {
            LOG.debug("Runner for pipeline '{}::{}' is in status: '{}'", runner.getName(), runner.getRev(),
              runner.getState());
            removeRunnerIfActive(runner);
          } catch (PipelineStoreException ex) {
            LOG.warn("Cannot remove runner for pipeline: '{}::{}' due to '{}'", runner.getName(), runner.getRev(),
              ex.toString(), ex);
          }
        }
      }
    }, runnerExpiryInterval, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  boolean isRunnerPresent(String name, String rev) {
     return runnerCache.getIfPresent(getNameAndRevString(name, rev)) == null? false: true;

  }

  private boolean removeRunnerIfActive(Runner runner) throws PipelineStoreException {
    if (!runner.getState().getStatus().isActive()) {
      runner.close();
      runnerCache.invalidate(getNameAndRevString(runner.getName(), runner.getRev()));
      LOG.info("Removing runner for pipeline '{}::'{}'", runner.getName(), runner.getRev());
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void stopTask() {
    for (RunnerInfo runnerInfo : runnerCache.asMap().values()) {
      Runner runner = runnerInfo.runner;
      try {
        runner.close();
        runner.onDataCollectorStop();
      } catch (Exception e) {
        LOG.warn("Failed to stop the runner for pipeline: {} and rev: {} due to: {}", runner.getName(),
          runner.getRev(), e.toString(), e);
      }
    }
    runnerCache.invalidateAll();
    for (Previewer previewer : previewerCache.asMap().values()) {
      try {
        previewer.stop();
      } catch (Exception e) {
        LOG.warn("Failed to stop the previewer: {}::{}::{} due to: {}", previewer.getName(),
          previewer.getRev(), previewer.getId(), e.toString(), e);
      }
    }
    previewerCache.invalidateAll();
    runnerExpiryFuture.cancel(true);
    LOG.info("Stopped Production Pipeline Manager");
  }

  @Override
  public void statusChange(String id, PreviewStatus status) {
    LOG.debug("Status of previewer with id: '{}' changed to status: '{}'", id, status);
  }

  @Override
  public void outputRetrieved(String id) {
    LOG.debug("Removing previewer with id:  '{}' from cache as output is retrieved", id);
    previewerCache.invalidate(id);
  }

  @Override
  public boolean isRemotePipeline(String name, String rev) throws PipelineStoreException {
    Object isRemote = pipelineStateStore.getState(name, rev).getAttributes().get(RemoteDataCollector.IS_REMOTE_PIPELINE);
    // remote attribute will be null for pipelines with version earlier than 1.3
    return (isRemote == null) ? false : (boolean) isRemote;
  }

  private Runner getRunner(String user, String name, String rev, ExecutionMode executionMode) throws PipelineStoreException {
    if(executionMode == null) {
      executionMode = ExecutionMode.STANDALONE;
    }
    Runner runner = runnerProvider.createRunner(user, name, rev, objectGraph, executionMode);
    return runner;
  }

  private String getNameAndRevString(String name, String rev) {
    return name + NAME_AND_REV_SEPARATOR + rev;
  }

  private static class RunnerInfo {
    private final Runner runner;
    private ExecutionMode executionMode;

    private RunnerInfo(Runner runner, ExecutionMode executionMode) {
      this.runner = runner;
      this.executionMode = executionMode;
    }
  }

}
