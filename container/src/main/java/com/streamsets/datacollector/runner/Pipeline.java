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
package com.streamsets.datacollector.runner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.memory.MemoryUsageCollectorResourceBundle;
import com.streamsets.datacollector.runner.production.BadRecordsHandler;
import com.streamsets.datacollector.runner.production.StatsAggregationHandler;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
  private final PipelineBean pipelineBean;
  private final String name;
  private final String rev;
  private final Configuration configuration;
  private final Pipe[] pipes;
  private final PipelineRunner runner;
  private final Observer observer;
  private final BadRecordsHandler badRecordsHandler;
  private final StatsAggregationHandler statsAggregationHandler;
  private final ResourceControlledScheduledExecutor scheduledExecutorService;
  private volatile boolean running;
  private boolean shouldStopOnStageError = false;

  private Pipeline(
      String name,
      String rev,
      Configuration configuration,
      PipelineBean pipelineBean,
      Pipe[] pipes,
      Observer observer,
      BadRecordsHandler badRecordsHandler,
      PipelineRunner runner,
      ResourceControlledScheduledExecutor scheduledExecutorService,
      StatsAggregationHandler statsAggregationHandler
  ) {
    this.pipelineBean = pipelineBean;
    this.name = name;
    this.rev = rev;
    this.configuration = configuration;
    this.pipes = pipes;
    this.observer = observer;
    this.badRecordsHandler = badRecordsHandler;
    this.runner = runner;
    this.scheduledExecutorService = scheduledExecutorService;
    this.running = false;
    this.statsAggregationHandler = statsAggregationHandler;
    for (Pipe pipe : pipes) {
      StageContext stageContext = (StageContext) pipe.getStage().getContext();
      if (stageContext.getOnErrorRecord() == OnRecordError.STOP_PIPELINE) {
        shouldStopOnStageError = true;
      }
    }
  }

  PipelineConfigBean getPipelineConfig() {
    return  pipelineBean.getConfig();
  }

  @VisibleForTesting
  Pipe[] getPipes() {
    return pipes;
  }

  public boolean shouldStopOnStageError() {
    return shouldStopOnStageError;
  }

  public Source getSource() {
    for (Pipe pipe : pipes) {
      if (pipe.getStage().getStage() instanceof Source) {
        return (Source)pipe.getStage().getStage();
      }
    }
    throw new NullPointerException("Cannot find pipeline source");
  }

  public PipelineRunner getRunner() {
    return runner;
  }

  public List<Issue> validateConfigs() throws StageException {
    try {
      return init();
    } catch (Throwable throwable) {
      LOG.error("Uncaught error in init: " + throwable, throwable);
      throw Throwables.propagate(throwable);
    } finally {
      destroy();
    }
  }

  @SuppressWarnings("unchecked")
  public List<Issue> init() {
    PipeContext pipeContext = new PipeContext();
    List<Issue> issues = new ArrayList<>();
    try {
      issues.addAll(badRecordsHandler.init(pipeContext));
    } catch (Exception ex) {
      LOG.warn(ContainerError.CONTAINER_0700.getMessage(), ex.toString(), ex);
      issues.add(IssueCreator.getStage(badRecordsHandler.getInstanceName()).create(ContainerError.CONTAINER_0700,
        ex.toString()));
    }
    if (statsAggregationHandler != null) {
      try {
        issues.addAll(statsAggregationHandler.init(pipeContext));
      } catch (Exception ex) {
        LOG.warn(ContainerError.CONTAINER_0703.getMessage(), ex.toString(), ex);
        issues.add(IssueCreator.getStage(statsAggregationHandler.getInstanceName()).create(ContainerError.CONTAINER_0703,
          ex.toString()));
      }
    }
    for (Pipe pipe : pipes) {
      try {
        issues.addAll(pipe.init(pipeContext));
      } catch (Exception ex) {
        String instanceName = pipe.getStage().getConfiguration().getInstanceName();
        LOG.warn(ContainerError.CONTAINER_0701.getMessage(), instanceName, ex.toString(), ex);
        issues.add(IssueCreator.getStage(instanceName).create(ContainerError.CONTAINER_0701, instanceName,
          ex.toString()));
      }
    }
    return issues;
  }

  public void errorNotification(Throwable throwable) {
    runner.errorNotification(pipes, throwable);
  }

  public void destroy() {
    try {
      runner.destroy(pipes, badRecordsHandler, statsAggregationHandler);
    } catch (StageException|PipelineRuntimeException ex) {
      String msg = Utils.format("Exception thrown in destroy phase: {}", ex.getMessage());
      LOG.warn(msg, ex);
    }
    try {
      badRecordsHandler.destroy();
    } catch (Exception ex) {
      String msg = Utils.format("Exception thrown during bad record handler destroy: {}", ex);
      LOG.warn(msg, ex);
    }
    try {
      if (statsAggregationHandler != null) {
        statsAggregationHandler.destroy();
      }
    } catch (Exception ex) {
      String msg = Utils.format("Exception thrown during Stats Aggregator handler destroy: {}", ex);
      LOG.warn(msg, ex);
    }
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdown();
    }
  }

  public void run() throws StageException, PipelineRuntimeException {
    this.running = true;
    try {
      runner.setObserver(observer);
      runner.run(pipes, badRecordsHandler, statsAggregationHandler);
    } finally {
      this.running = false;
    }
  }

  public void run(List<StageOutput> stageOutputsToOverride) throws StageException, PipelineRuntimeException {
    this.running = true;
    try {
    runner.setObserver(observer);
    runner.run(pipes, badRecordsHandler, stageOutputsToOverride, statsAggregationHandler);
    } finally {
      this.running = false;
    }
  }

  public boolean isRunning() {
    return running;
  }

  public void stop() {
    for(Pipe p : pipes) {
      ((StageContext)p.getStage().getContext()).setStop(true);
    }
  }

  public static class Builder {
    private static final String EXECUTION_MODE_CONFIG_KEY = "executionMode";
    private static final String EXECUTION_MODE_CLUSTER = "CLUSTER";

    private final StageLibraryTask stageLib;
    private final Configuration configuration;
    private final String name;
    private final String pipelineName;
    private final String rev;
    private final PipelineConfiguration pipelineConf;
    private Observer observer;
    private final ResourceControlledScheduledExecutor scheduledExecutor =
      new ResourceControlledScheduledExecutor(0.01f); // consume 1% of a cpu calculating stage memory consumption
    private final MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle =
      new MemoryUsageCollectorResourceBundle();
    private List<Issue> errors;


    public Builder(StageLibraryTask stageLib, Configuration configuration, String name, String pipelineName, String rev,
                   PipelineConfiguration pipelineConf) {
      this.stageLib = stageLib;
      this.name = name;
      this.pipelineName = pipelineName;
      this.rev = rev;
      this.configuration = configuration;
      this.pipelineConf = pipelineConf;
      errors = Collections.emptyList();
    }
    public Builder setObserver(Observer observer) {
      this.observer = observer;
      return this;
    }

    public Pipeline build(PipelineRunner runner) throws PipelineRuntimeException {
      Pipeline pipeline = null;
      errors = new ArrayList<>();
      PipelineBean pipelineBean = PipelineBeanCreator.get().create(true, stageLib, pipelineConf, errors);
      StageRuntime[] stages;
      StageRuntime errorStage;
      StageRuntime statsAggregator = null;
      if (pipelineBean != null) {
        stages = new StageRuntime[pipelineBean.getStages().size()];
        for (int i = 0; i < pipelineBean.getStages().size(); i++) {
          stages[i] = new StageRuntime(pipelineBean, pipelineBean.getStages().get(i));
        }
        errorStage = new StageRuntime(pipelineBean, pipelineBean.getErrorStage());
        StatsAggregationHandler statsAggregationHandler = null;
        if (pipelineBean.getStatsAggregatorStage() != null) {
          statsAggregator = new StageRuntime(pipelineBean, pipelineBean.getStatsAggregatorStage());
          statsAggregationHandler = new StatsAggregationHandler(statsAggregator);
        }
        setStagesContext(stages, errorStage, statsAggregator, runner);
        Pipe[] pipes = createPipes(stages, runner);
        BadRecordsHandler badRecordsHandler = new BadRecordsHandler(errorStage);

        try {
          pipeline = new Pipeline(
              name,
              rev,
              configuration,
              pipelineBean,
              pipes,
              observer,
              badRecordsHandler,
              runner,
              scheduledExecutor,
              statsAggregationHandler
          );
        } catch (Exception e) {
          String msg = "Could not create memory usage collector: " + e;
          throw new PipelineRuntimeException(ContainerError.CONTAINER_0151, msg, e);
        }
      }
      return pipeline;
    }

    public List<Issue> getIssues() {
      return errors;
    }

    private void setStagesContext(
        StageRuntime[] stages,
        StageRuntime errorStage,
        StageRuntime statsAggregatorStage,
        PipelineRunner runner
    ) {
      List<Stage.Info> infos = new ArrayList<>(stages.length);
      List<Stage.Info> infosUnmodifiable = Collections.unmodifiableList(infos);
      ExecutionMode executionMode = getExecutionMode(pipelineConf);
      for (StageRuntime stage : stages) {
        infos.add(stage.getInfo());
        stage.setContext(
            new StageContext(
                pipelineName,
                rev,
                infosUnmodifiable,
                stage.getDefinition().getType(),
                runner.isPreview(),
                runner.getMetrics(),
                stage,
                pipelineConf.getMemoryLimitConfiguration().getMemoryLimit(),
                executionMode,
                runner.getRuntimeInfo().getResourcesDir(),
                new EmailSender(configuration)
            )
        );
      }
      errorStage.setContext(
          new StageContext(
              pipelineName,
              rev,
              infosUnmodifiable,
              errorStage.getDefinition().getType(),
              runner.isPreview(),
              runner.getMetrics(),
              errorStage,
              pipelineConf.getMemoryLimitConfiguration().getMemoryLimit(),
              executionMode,
              runner.getRuntimeInfo().getResourcesDir(),
              new EmailSender(configuration)
          )
      );
      if (statsAggregatorStage != null) {
        statsAggregatorStage.setContext(
            new StageContext(
                pipelineName,
                rev,
                infosUnmodifiable,
                statsAggregatorStage.getDefinition().getType(),
                runner.isPreview(),
                runner.getMetrics(),
                statsAggregatorStage,
                pipelineConf.getMemoryLimitConfiguration().getMemoryLimit(),
                executionMode,
                runner.getRuntimeInfo().getResourcesDir(),
                new EmailSender(configuration)
            )
        );
      }
    }

    private ExecutionMode getExecutionMode(PipelineConfiguration pipelineConf) {
      Config executionModeConfig =
        Utils.checkNotNull(pipelineConf.getConfiguration(EXECUTION_MODE_CONFIG_KEY), EXECUTION_MODE_CONFIG_KEY);
      String executionMode = executionModeConfig.getValue().toString();
      Utils.checkState(executionMode != null && !executionMode.isEmpty(), "Execution mode cannot be null or empty");
      return ExecutionMode.valueOf(executionMode);

    }

    private Pipe[] createPipes(StageRuntime[] stages, PipelineRunner runner) throws PipelineRuntimeException {
      LaneResolver laneResolver = new LaneResolver(stages);
      List<Pipe> pipes = new ArrayList<>(stages.length * 3);

      for (int idx = 0; idx < stages.length; idx++) {
        Pipe pipe;
        StageRuntime stage = stages[idx];
        switch (stage.getDefinition().getType()) {
          case SOURCE:
            pipe = new StagePipe(pipelineName, rev, configuration, stage, laneResolver.getStageInputLanes(idx),
              laneResolver.getStageOutputLanes(idx), laneResolver.getStageEventLanes(idx), scheduledExecutor, memoryUsageCollectorResourceBundle, runner.getMetricRegistryJson());
            pipes.add(pipe);
            pipe = new ObserverPipe(stage, laneResolver.getObserverInputLanes(idx),
                                    laneResolver.getObserverOutputLanes(idx), observer);
            pipes.add(pipe);
            pipe = new MultiplexerPipe(stage, laneResolver.getMultiplexerInputLanes(idx),
                                       laneResolver.getMultiplexerOutputLanes(idx));
            pipes.add(pipe);
            break;
          case PROCESSOR:
            pipe = new CombinerPipe(stage, laneResolver.getCombinerInputLanes(idx),
                                    laneResolver.getCombinerOutputLanes(idx));
            pipes.add(pipe);
            pipe = new StagePipe(pipelineName, rev, configuration, stage, laneResolver.getStageInputLanes(idx),
                                 laneResolver.getStageOutputLanes(idx), laneResolver.getStageEventLanes(idx), scheduledExecutor,
              memoryUsageCollectorResourceBundle, runner.getMetricRegistryJson());
            pipes.add(pipe);
            pipe = new ObserverPipe(stage, laneResolver.getObserverInputLanes(idx),
                                    laneResolver.getObserverOutputLanes(idx), observer);
            pipes.add(pipe);
            pipe = new MultiplexerPipe(stage, laneResolver.getMultiplexerInputLanes(idx),
                                       laneResolver.getMultiplexerOutputLanes(idx));
            pipes.add(pipe);
            break;
          case TARGET:
            pipe = new CombinerPipe(stage, laneResolver.getCombinerInputLanes(idx),
                                    laneResolver.getCombinerOutputLanes(idx));
            pipes.add(pipe);
            pipe = new StagePipe(pipelineName, rev, configuration, stage, laneResolver.getStageInputLanes(idx),
              laneResolver.getStageOutputLanes(idx), laneResolver.getStageEventLanes(idx), scheduledExecutor, memoryUsageCollectorResourceBundle, runner.getMetricRegistryJson());
            pipes.add(pipe);

            // In case that this target is generating events, we need to add additional observer/multiplexer pipe
            if(stage.getConfiguration().getEventLanes().size() > 0) {
              pipe = new ObserverPipe(stage, laneResolver.getObserverInputLanes(idx),
                                      laneResolver.getObserverOutputLanes(idx), observer);
              pipes.add(pipe);
              pipe = new MultiplexerPipe(stage, laneResolver.getMultiplexerInputLanes(idx),
                                         laneResolver.getMultiplexerOutputLanes(idx));
              pipes.add(pipe);
            }

            break;
          default:
            throw new IllegalStateException("Unexpected DefinitionType " + stage.getDefinition().getType());
        }
      }
      return pipes.toArray(new Pipe[pipes.size()]);
    }

  }

  @Override
  public String toString() {
    Set<String> instances = new LinkedHashSet<>();
    for (Pipe pipe : pipes) {
      instances.add(pipe.getStage().getInfo().getInstanceName());
    }
    String observerName = (observer != null) ? observer.getClass().getSimpleName() : null;
    return Utils.format("Pipeline[stages='{}' runner='{}' observer='{}']", instances, runner.getClass().getSimpleName(),
                        observerName);
  }

}
