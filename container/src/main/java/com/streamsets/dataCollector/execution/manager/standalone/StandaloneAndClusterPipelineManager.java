/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.manager.standalone;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.streamsets.dataCollector.execution.Manager;
import com.streamsets.dataCollector.execution.PipelineState;
import com.streamsets.dataCollector.execution.PipelineStateStore;
import com.streamsets.dataCollector.execution.PipelineStatus;
import com.streamsets.dataCollector.execution.PreviewStatus;
import com.streamsets.dataCollector.execution.Previewer;
import com.streamsets.dataCollector.execution.PreviewerListener;
import com.streamsets.dataCollector.execution.Runner;
import com.streamsets.dataCollector.execution.manager.PreviewerProvider;
import com.streamsets.dataCollector.execution.manager.RunnerProvider;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.creation.PipelineBeanCreator;
import com.streamsets.pipeline.creation.PipelineConfigBean;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.validation.Issue;

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
  @Inject
  RunnerProvider runnerProvider;
  @Inject
  PreviewerProvider previewerProvider;

  private Cache<String, Runner> runnerCache;
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
  }

  @Override
  public Previewer createPreviewer(String user, String name, String rev) {
    Previewer previewer = previewerProvider.createPreviewer(user, name, rev, this, objectGraph);
    previewerCache.put(previewer.getId(), previewer);
    return previewer;
  }

  @Override
  public Previewer getPreview(String previewerId) {
    Utils.checkNotNull(previewerId, "previewerId");
    Previewer previewer = previewerCache.getIfPresent(previewerId);
    if (previewer == null) {
      LOG.warn("Cannot find the previewer in cache for id: '{}'", previewerId);
    }
    return previewer;
  }

  @Override
  public Runner getRunner(final String user, final String name, final String rev) throws PipelineStoreException {
    final String nameAndRevString = getNameAndRevString(name, rev);
    Runner runner;
    try {
      runner = runnerCache.get(nameAndRevString, new Callable<Runner>() {
        @Override
        public Runner call() throws PipelineStoreException {
          return getRunner(pipelineStateStore.getState(name, rev), name, rev);
        }
      });
    } catch (ExecutionException ex) {
      if (ex.getCause() instanceof RuntimeException) {
        throw (RuntimeException) ex.getCause();
      } else if (ex.getCause() instanceof PipelineStoreException) {
        throw (PipelineStoreException) ex.getCause();
      } else {
        throw new PipelineStoreException(ContainerError.CONTAINER_0114, ex.getMessage(), ex);
      }
    }
    return runner;
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
    Runner runner = runnerCache.getIfPresent(getNameAndRevString(name, rev));
    return (runner == null) ? false : runner.getStatus().isActive();
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
          Runner runner = getRunner(pipelineState, name, rev);
          runner.prepareForDataCollectorStart();
          if (runner.getStatus() == PipelineStatus.DISCONNECTED) {
            runnerCache.put(getNameAndRevString(name, rev), runner);
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
        for (Runner runner : runnerCache.asMap().values()) {
          try {
            LOG.debug("Runner for pipeline '{}::{}' is in status: '{}'", runner.getName(), runner.getRev(),
              runner.getStatus());
            if (!runner.getStatus().isActive()) {
              runner.close();
              runnerCache.invalidate(getNameAndRevString(runner.getName(), runner.getRev()));
              LOG.info("Removing runner for pipeline '{}::'{}'", runner.getName(), runner.getRev());
            }
          } catch (PipelineStoreException ex) {
            LOG.warn("Cannot remove runner for pipeline: '{}::{}' due to '{}'", runner.getName(), runner.getRev(),
              ex.getMessage(), ex);
          }
        }
      }
    }, runnerExpiryInterval, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  boolean isRunnerPresent(String name, String rev) {
     return runnerCache.getIfPresent(getNameAndRevString(name, rev)) == null? false: true;

  }

  @Override
  public void stopTask() {
    for (Runner runner : runnerCache.asMap().values()) {
      try {
        runner.close();
        runner.onDataCollectorStop();
      } catch (Exception e) {
        LOG.warn("Failed to stop the runner for pipeline: {} and rev: {} due to: {}", runner.getName(),
          runner.getRev(), e.getMessage(), e);
      }
    }
    runnerCache.invalidateAll();
    for (Previewer previewer : previewerCache.asMap().values()) {
      try {
        previewer.stop();
      } catch (Exception e) {
        LOG.warn("Failed to stop the previewer: {}::{}::{} due to: {}", previewer.getName(),
          previewer.getRev(), previewer.getId(), e.getMessage(), e);
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


  private Runner getRunner(PipelineState pipelineState, String name, String rev) throws PipelineStoreException {
    LOG.debug("Status of pipeline: '{}::{}' is: '{}'", name, rev, pipelineState.getStatus());
    List<Issue> errors = new ArrayList<>();
    PipelineConfiguration pipelineConf = pipelineStore.load(name, rev);
    PipelineConfigBean pipelineConfigBean = PipelineBeanCreator.get().create(pipelineConf, errors);
    if (pipelineConfigBean == null) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0116, errors);
    }
    return runnerProvider.createRunner(pipelineState.getUser(), name, rev, pipelineConfigBean, objectGraph);
  }

  private String getNameAndRevString(String name, String rev) {
    return name + NAME_AND_REV_SEPARATOR + rev;
  }

}
