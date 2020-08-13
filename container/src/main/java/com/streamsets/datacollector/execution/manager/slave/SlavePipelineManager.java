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
package com.streamsets.datacollector.execution.manager.slave;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.execution.manager.RunnerProvider;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.SlaveRuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

import dagger.ObjectGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SlavePipelineManager extends AbstractTask implements Manager {
  private static final Logger LOG = LoggerFactory.getLogger(SlavePipelineManager.class);
  private static final String SLAVE_MANAGER = "SlaveManager";

  private final ObjectGraph objectGraph;
  @Inject RuntimeInfo runtimeInfo;
  @Inject Configuration configuration;
  @Inject PipelineStateStore pipelineStateStore;
  @Inject @Named("runnerExecutor") SafeScheduledExecutorService runnerExecutor;
  @Inject RunnerProvider runnerProvider;
  @Inject EventListenerManager eventListenerManager;
  private Runner runner;

  public SlavePipelineManager(ObjectGraph objectGraph) {
    super(SLAVE_MANAGER);
    this.objectGraph = objectGraph;
    this.objectGraph.inject(this);
    MetricsConfigurator.registerJmxMetrics(runtimeInfo.getMetrics());
  }

  @Override
  public Previewer createPreviewer(
      String user,
      String name,
      String rev,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
      Function<Object, Void> afterActionsFunction,
      boolean remote,
      Map<String, ConnectionConfiguration> connections
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Previewer getPreviewer(String previewerId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Runner getRunner(String name, String rev) throws PipelineStoreException {
    if (runner != null && runner.getName().equals(name) && runner.getRev().equals(rev)) {
      return runner;
    }
    if (runner != null) {
      throw new IllegalStateException(Utils.format("Cannot create runner for '{}::{}', only one "
        + "runner allowed in a slave SDC", name, rev));
    }
    runner = runnerProvider.createRunner(name, rev, objectGraph, null);
    // Set the initial state
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(RemoteDataCollector.IS_REMOTE_PIPELINE, ((SlaveRuntimeInfo) runtimeInfo).isRemotePipeline());
    // This is setting initial state on a slave node and hence we use empty user
    pipelineStateStore.saveState("", name, rev, PipelineStatus.EDITED, null, attributes, ExecutionMode.SLAVE, null, 0, 0);
    return runner;
  }

  @Override
  public List<PipelineState> getPipelines() throws PipelineStoreException {
    List<PipelineState> pipelineStates = new ArrayList<>(1);
    if(runner != null) {
      pipelineStates.add(runner.getState());
    }
    return pipelineStates;
  }

  public PipelineState getPipelineState(String name, String rev) throws PipelineStoreException {
    return runner.getState();
  }

  @Override
  public boolean isPipelineActive(String name, String rev) throws PipelineStoreException {
    return (runner == null) ? false : runner.getState().getStatus().isActive();
  }

  @Override
  public void stopTask() {
    if (runner != null) {
      try {
        // This is ephemeral copy of pipeline on a worker slave, so we run the stop with empty user name
        runner.onDataCollectorStop("");
      } catch (Exception ex) {
        LOG.error(
          Utils.format("Cannot stop runner for pipeline '{}::{}' due to '{}'", runner.getName(), runner.getRev(), ex),
          ex);
      }
    }
  }

  @Override
  public boolean isRemotePipeline(String name, String rev) throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addStateEventListener(StateEventListener listener) {
    throw new UnsupportedOperationException();
  }

}
