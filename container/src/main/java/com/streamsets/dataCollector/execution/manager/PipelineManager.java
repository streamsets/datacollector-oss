/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.streamsets.dataCollector.execution.Manager;
import com.streamsets.dataCollector.execution.PipelineState;
import com.streamsets.dataCollector.execution.PipelineStateStore;
import com.streamsets.dataCollector.execution.PreviewStatus;
import com.streamsets.dataCollector.execution.Previewer;
import com.streamsets.dataCollector.execution.PreviewerListener;
import com.streamsets.dataCollector.execution.Runner;
import com.streamsets.dataCollector.execution.preview.SyncPreviewer;
import com.streamsets.dataCollector.execution.runner.ClusterRunner;
import com.streamsets.dataCollector.execution.runner.StandaloneRunner;

import com.streamsets.dataCollector.execution.util.PipelineStatusUtil;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.definition.PipelineDefConfigs;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.StandalonePipelineManagerTask;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.Configuration;

public class PipelineManager extends AbstractTask implements Manager, PreviewerListener  {

  private static final Logger LOG = LoggerFactory.getLogger(StandalonePipelineManagerTask.class);
  private static final String PIPELINE_MANAGER = "PipelineManager";
  private final RuntimeInfo runtimeInfo;
  private final Configuration configuration;
  private final PipelineStoreTask pipelineStore;
  private final PipelineStateStore pipelineStateStore;
  private ConcurrentMap<String, Runner> runnerMap = new ConcurrentHashMap<String, Runner>();
  private Cache<String, Previewer> previewerCache;
  private final StageLibraryTask stageLibrary;
  private final SafeScheduledExecutorService previewExecutor;
  private final SafeScheduledExecutorService runnerExecutor;

  @Inject
  public PipelineManager(RuntimeInfo runtimeInfo,
      Configuration configuration, PipelineStoreTask pipelineStore, PipelineStateStore pipelineStateStore,
      StageLibraryTask stageLibrary, SafeScheduledExecutorService previewExecutor, SafeScheduledExecutorService runnerExecutor) {
    super(PIPELINE_MANAGER);
    this.runtimeInfo = runtimeInfo;
    this.configuration = configuration;
    this.pipelineStore = pipelineStore;
    this.pipelineStateStore = pipelineStateStore;
    this.stageLibrary = stageLibrary;
    this.previewExecutor = previewExecutor;
    this.runnerExecutor = runnerExecutor;
  }

  @Override
  public Previewer createPreviewer(String name, String rev) {
    UUID uuid = UUID.randomUUID();
    // pass the previewExecutor
    Previewer previewer =
      new SyncPreviewer(uuid.toString(), name, rev, this, configuration, stageLibrary, pipelineStore, runtimeInfo);
    previewerCache.put(uuid.toString(), previewer);
    return previewer;
  }

  @Override
  public Previewer getPreview(String previewerId) {
    Utils.checkNotNull(previewerId, "previewerId");
    Previewer previewer = previewerCache.getIfPresent(previewerId);
    if (previewer == null) {
      LOG.warn("Cannot find the previewer in cached for id: " + previewerId);
    }
    return previewer;
  }

  private String getNameAndRevString(String name, String rev) {
     return name + "::" + rev;
  }

  @Override
  public Runner getRunner(String name, String rev, String user) throws PipelineStoreException {
    Runner runner;
    String nameAndRevString = getNameAndRevString(name, rev);
    if (runnerMap.containsKey(nameAndRevString)) {
      runner = runnerMap.get(nameAndRevString);
    } else {
      PipelineConfiguration pipelineConf = pipelineStore.load(name, rev);
      ExecutionMode executionMode =
        ExecutionMode.valueOf((String) pipelineConf.getConfiguration(PipelineDefConfigs.EXECUTION_MODE_CONFIG)
          .getValue());
      switch (executionMode) {
        case CLUSTER:
          // pass the scheduled executor and runner executor
          runner = new ClusterRunner();
          break;
        case STANDALONE:
          runner = new StandaloneRunner(name, rev, user, runtimeInfo, configuration, pipelineStore, pipelineStateStore, stageLibrary, runnerExecutor);
          break;
        default:
          throw new IllegalArgumentException(Utils.format("Invalid execution mode '{}'", executionMode));
      }
      Runner alreadyPresentRunner = runnerMap.putIfAbsent(nameAndRevString, runner);
      if (alreadyPresentRunner != null) {
        runner = alreadyPresentRunner;
      }
    }
    return runner;
  }

  @Override
  public List<PipelineState> getPipelines() throws PipelineStoreException {
    List<PipelineInfo> pipelineInfoList = pipelineStore.getPipelines();
    List<PipelineState> pipelineStateList = new ArrayList<PipelineState>();
    for (PipelineInfo pipelineInfo : pipelineInfoList) {
      String name = pipelineInfo.getName();
      String rev = pipelineInfo.getLastRev();
      PipelineState pipelineState = pipelineStateStore.getState(name, rev);
      Utils.checkState(pipelineState != null, "State for pipeline: " + name + " of revision " + rev + " doesn't exist");
      pipelineStateList.add(pipelineState);
    }
    return pipelineStateList;
  }

  @Override
  public boolean isPipelineActive(String name, String rev) throws PipelineStoreException {
    PipelineState pipelineState = pipelineStateStore.getState(name, rev);
    return PipelineStatusUtil.isActive(pipelineState.getStatus());
  }

  @Override
  public void initTask() {
    previewerCache = CacheBuilder.newBuilder()
      .expireAfterAccess(30, TimeUnit.MINUTES).build();
    List<PipelineInfo> pipelineInfoList;
    try {
      pipelineInfoList = pipelineStore.getPipelines();
      for (PipelineInfo pipelineInfo : pipelineInfoList) {
        String name = pipelineInfo.getName();
        String rev = pipelineInfo.getLastRev();
        PipelineState pipelineState = pipelineStateStore.getState(name, rev);
        // Create runner if active
        if (PipelineStatusUtil.isActive(pipelineState.getStatus())) {
          LOG.debug("Pipeline status of pipeline: " + name + " and rev " + rev + " is:" + pipelineState.getStatus());
          ExecutionMode executionMode = pipelineState.getExecutionMode();
          Runner runner;
          switch (executionMode) {
            case CLUSTER:
              runner = new ClusterRunner();
              break;
            case STANDALONE:
              runner = new StandaloneRunner(name, rev, pipelineState.getUser(), runtimeInfo, configuration, pipelineStore, pipelineStateStore, stageLibrary, runnerExecutor);
              break;
            default:
              throw new IllegalArgumentException(Utils.format("Invalid execution mode '{}'", executionMode));
          }
          Runner alreadyPresentRunner = runnerMap.putIfAbsent(getNameAndRevString(name, rev), runner);
          if (alreadyPresentRunner != null) {
            runner = alreadyPresentRunner;
          }
          runner.onDataCollectorStart();
        }
      }
    } catch (Exception e) {
       throw new RuntimeException("Exception while initializing manager: " + e.getMessage(), e);
    }
  }

  @VisibleForTesting
  boolean isRunnerCreated(String name, String rev) {
     return runnerMap.containsKey(getNameAndRevString(name, rev));
  }

  @Override
  protected void stopTask() {
    previewerCache.invalidateAll();
    for (Runner runner : runnerMap.values()) {
      try {
        runner.onDataCollectorStop();
      } catch (Exception e) {
        LOG.warn("Failed to stop the runner for pipeline: {} and rev: {} due to: {}", runner.getName(),
          runner.getRev(), e.getMessage(), e);
      }
    }
    runnerMap.clear();
    if (runnerExecutor != null) {
      runnerExecutor.shutdown();
      try {
        if (!runnerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
          runnerExecutor.shutdownNow();
          if (!runnerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
            LOG.error("Runner Executor did not terminate");
          }
        }
      } catch (InterruptedException e) {
        LOG.warn(Utils.format("Forced termination. Reason {}", e.getMessage()));
      }
    }
    if (previewExecutor != null) {
      previewExecutor.shutdown();
      try {
        if (!previewExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
          previewExecutor.shutdownNow();
          if (!previewExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
            LOG.error("Preview Executor did not terminate");
          }
        }
      } catch (InterruptedException e) {
        previewExecutor.shutdownNow();
        LOG.warn(Utils.format("Forced termination. Reason {}", e.getMessage()));
      }
    }
    LOG.debug("Stopped Production Pipeline Manager");
  }

  @Override
  public void statusChange(String id, PreviewStatus status) {
    LOG.info("Status of previewer with id: " + id + " changed to " + status);
  }

  @Override
  public void outputRetrieved(String id) {
    LOG.info("Removing previewer with id: " + id + " from cache as output is retrieved");
    previewerCache.invalidate(id);
  }
}
