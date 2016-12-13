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
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.creation.PipelineStageBeans;
import com.streamsets.datacollector.creation.StageBean;
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
import com.streamsets.pipeline.api.ProtoSource;
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
  private final Pipe originPipe;
  private final List<List<Pipe>> pipes;
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
      Pipe originPipe,
      List<List<Pipe>> pipes,
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
    this.originPipe = originPipe;
    this.pipes = pipes;
    this.observer = observer;
    this.badRecordsHandler = badRecordsHandler;
    this.runner = runner;
    this.scheduledExecutorService = scheduledExecutorService;
    this.running = false;
    this.statsAggregationHandler = statsAggregationHandler;
    this.shouldStopOnStageError = calculateShouldStopOnStageError();
  }

  PipelineConfigBean getPipelineConfig() {
    return pipelineBean.getConfig();
  }

  // TODO: To be removed in subsequent patches
  @VisibleForTesting
  Pipe[] getPipes() {
    List<Pipe> p = new ArrayList<>(1 + pipes.get(0).size());
    p.add(originPipe);
    p.addAll(pipes.get(0));
    return p.toArray(new Pipe[p.size()]);
  }

  private boolean calculateShouldStopOnStageError() {
    if(shouldPipeStopPipeline(originPipe)) {
      return true;
    }

    for(Pipe pipe : pipes.get(0)) {
      if(shouldPipeStopPipeline(pipe)) {
        return true;
      }
    }

    return false;
  }

  private static boolean shouldPipeStopPipeline(Pipe pipe) {
    StageContext stageContext = pipe.getStage().getContext();
    return stageContext.getOnErrorRecord() == OnRecordError.STOP_PIPELINE;
  }

  public boolean shouldStopOnStageError() {
    return shouldStopOnStageError;
  }

  public ProtoSource getSource() {
    return (ProtoSource) originPipe.getStage().getStage();
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

    issues.addAll(initPipe(originPipe, pipeContext));
    for(List<Pipe> runnerPipes: pipes) {
      for(Pipe pipe : runnerPipes) {
        issues.addAll(initPipe(pipe, pipeContext));
      }
    }
    this.runner.setPipeContext(pipeContext);
    return issues;
  }

  private List<Issue> initPipe(Pipe pipe, PipeContext pipeContext) {
    try {
      return pipe.init(pipeContext);
    } catch (Exception ex) {
      String instanceName = originPipe.getStage().getConfiguration().getInstanceName();
      LOG.warn(ContainerError.CONTAINER_0701.getMessage(), instanceName, ex.toString(), ex);
      return ImmutableList.of(
        IssueCreator
          .getStage(instanceName)
          .create(ContainerError.CONTAINER_0701, instanceName, ex.toString()
          )
      );
    }
  }

  public void errorNotification(Throwable throwable) {
    runner.errorNotification(originPipe, pipes, throwable);
  }

  public void destroy() {
    try {
      runner.destroy(originPipe, pipes, badRecordsHandler, statsAggregationHandler);
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
      runner.run(originPipe, pipes, badRecordsHandler, statsAggregationHandler);
    } finally {
      this.running = false;
    }
  }

  public void run(List<StageOutput> stageOutputsToOverride) throws StageException, PipelineRuntimeException {
    this.running = true;
    try {
      runner.setObserver(observer);
      runner.run(originPipe, pipes, badRecordsHandler, stageOutputsToOverride, statsAggregationHandler);
    } finally {
      this.running = false;
    }
  }

  public boolean isRunning() {
    return running;
  }

  public void stop() {
    ((StageContext)originPipe.getStage().getContext()).setStop(true);
    for(List<Pipe> runnerPipes : pipes) {
      for(Pipe p : runnerPipes) {
        ((StageContext)p.getStage().getContext()).setStop(true);
      }
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
      StageRuntime errorStage;
      StageRuntime statsAggregator = null;
      List<List<Pipe>> pipes = new ArrayList<>();
      if (pipelineBean != null) {
        Preconditions.checkArgument(!pipelineBean.getPipelineStageBeans().isEmpty(), "At least one instance of pipeline must exist!");

        // Origin runtime and pipe
        StageRuntime originRuntime = new StageRuntime(pipelineBean, pipelineBean.getOrigin());
        Pipe originPipe = createOriginPipe(originRuntime, runner);

        // Generate runtime and pipe for rest of the pipelines
        for(PipelineStageBeans beans : pipelineBean.getPipelineStageBeans()) {
          // Create runtime structures
          StageRuntime[] stages = new StageRuntime[1 + beans.size()];
          stages[0] = originRuntime;
          int i = 1;
          for(StageBean stageBean : beans.getStages()) {
            stages[i] = new StageRuntime(pipelineBean, stageBean);
            i++;
          }

          pipes.add(createPipes(stages, runner));

        }

        errorStage = new StageRuntime(pipelineBean, pipelineBean.getErrorStage());
        StatsAggregationHandler statsAggregationHandler = null;
        if (pipelineBean.getStatsAggregatorStage() != null) {
          statsAggregator = new StageRuntime(pipelineBean, pipelineBean.getStatsAggregatorStage());
          statsAggregationHandler = new StatsAggregationHandler(statsAggregator);
        }

        setStagesContext(originPipe, pipes, errorStage, statsAggregator, runner);
        BadRecordsHandler badRecordsHandler = new BadRecordsHandler(errorStage);

        try {
          pipeline = new Pipeline(
              name,
              rev,
              configuration,
              pipelineBean,
              originPipe,
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
      Pipe originPipe,
      List<List<Pipe>> pipes,
      StageRuntime errorStage,
      StageRuntime statsAggregatorStage,
      PipelineRunner runner
    ) {
      List<Stage.Info> infos = new ArrayList<>();
      List<Stage.Info> infosUnmodifiable = Collections.unmodifiableList(infos);
      ExecutionMode executionMode = getExecutionMode(pipelineConf);

      // Origin
      infos.add(originPipe.getStage().getInfo());
      originPipe.getStage().setContext(
        new StageContext(
            pipelineName,
            rev,
            infosUnmodifiable,
            originPipe.getStage().getDefinition().getType(),
            runner.isPreview(),
            runner.getMetrics(),
            originPipe.getStage(),
            pipelineConf.getMemoryLimitConfiguration().getMemoryLimit(),
            executionMode,
            runner.getRuntimeInfo().getResourcesDir(),
            new EmailSender(configuration),
            configuration
        )
      );

      // Non-origin stages
      for(List<Pipe> runnerPipes : pipes) {
        for (Pipe pipe : runnerPipes) {
          if (pipe instanceof StagePipe) {
            infos.add(pipe.getStage().getInfo());
            pipe.getStage().setContext(new StageContext(
              pipelineName,
              rev,
              infosUnmodifiable,
              pipe.getStage().getDefinition().getType(),
              runner.isPreview(),
              runner.getMetrics(),
              pipe.getStage(),
              pipelineConf.getMemoryLimitConfiguration().getMemoryLimit(),
              executionMode,
              runner.getRuntimeInfo().getResourcesDir(),
              new EmailSender(configuration),
              configuration
            ));
          }
        }
      }

      // Error stage
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
              new EmailSender(configuration),
              configuration
          )
      );

      // Aggregation stage is optional
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
                new EmailSender(configuration),
                configuration
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

    private Pipe createOriginPipe(StageRuntime originRuntime, PipelineRunner runner) {
      LaneResolver laneResolver = new LaneResolver(new StageRuntime[]{originRuntime});
      return new StagePipe(
        pipelineName,
        rev,
        configuration,
        originRuntime,
        laneResolver.getStageInputLanes(0),
        laneResolver.getStageOutputLanes(0),
        laneResolver.getStageEventLanes(0),
        scheduledExecutor,
        memoryUsageCollectorResourceBundle,
        runner.getMetricRegistryJson()
      );
    }

    private List<Pipe> createPipes(StageRuntime[] stages, PipelineRunner runner) throws PipelineRuntimeException {
      LaneResolver laneResolver = new LaneResolver(stages);
      ImmutableList.Builder<Pipe> pipesBuilder = ImmutableList.builder();

      for (int idx = 0; idx < stages.length; idx++) {
        Pipe pipe;
        StageRuntime stage = stages[idx];
        switch (stage.getDefinition().getType()) {
          case SOURCE:
            pipe = new ObserverPipe(stage, laneResolver.getObserverInputLanes(idx),
                                    laneResolver.getObserverOutputLanes(idx), observer);
            pipesBuilder.add(pipe);
            pipe = new MultiplexerPipe(stage, laneResolver.getMultiplexerInputLanes(idx),
                                       laneResolver.getMultiplexerOutputLanes(idx));
            pipesBuilder.add(pipe);
            break;
          case PROCESSOR:
            pipe = new CombinerPipe(stage, laneResolver.getCombinerInputLanes(idx),
                                    laneResolver.getCombinerOutputLanes(idx));
            pipesBuilder.add(pipe);
            pipe = new StagePipe(pipelineName, rev, configuration, stage, laneResolver.getStageInputLanes(idx),
                                 laneResolver.getStageOutputLanes(idx), laneResolver.getStageEventLanes(idx), scheduledExecutor,
              memoryUsageCollectorResourceBundle, runner.getMetricRegistryJson());
            pipesBuilder.add(pipe);
            pipe = new ObserverPipe(stage, laneResolver.getObserverInputLanes(idx),
                                    laneResolver.getObserverOutputLanes(idx), observer);
            pipesBuilder.add(pipe);
            pipe = new MultiplexerPipe(stage, laneResolver.getMultiplexerInputLanes(idx),
                                       laneResolver.getMultiplexerOutputLanes(idx));
            pipesBuilder.add(pipe);
            break;
          case EXECUTOR:
          case TARGET:
            pipe = new CombinerPipe(stage, laneResolver.getCombinerInputLanes(idx),
                                    laneResolver.getCombinerOutputLanes(idx));
            pipesBuilder.add(pipe);
            pipe = new StagePipe(pipelineName, rev, configuration, stage, laneResolver.getStageInputLanes(idx),
              laneResolver.getStageOutputLanes(idx), laneResolver.getStageEventLanes(idx), scheduledExecutor, memoryUsageCollectorResourceBundle, runner.getMetricRegistryJson());
            pipesBuilder.add(pipe);

            // In case that this target is generating events, we need to add additional observer/multiplexer pipe
            if(stage.getConfiguration().getEventLanes().size() > 0) {
              pipe = new ObserverPipe(stage, laneResolver.getObserverInputLanes(idx),
                                      laneResolver.getObserverOutputLanes(idx), observer);
              pipesBuilder.add(pipe);
              pipe = new MultiplexerPipe(stage, laneResolver.getMultiplexerInputLanes(idx),
                                         laneResolver.getMultiplexerOutputLanes(idx));
              pipesBuilder.add(pipe);
            }

            break;
          default:
            throw new IllegalStateException("Unexpected DefinitionType " + stage.getDefinition().getType());
        }
      }
      return pipesBuilder.build();
    }

  }

  @Override
  public String toString() {
    Set<String> instances = new LinkedHashSet<>();
    for (Pipe pipe : getPipes()) {
      instances.add(pipe.getStage().getInfo().getInstanceName());
    }
    String observerName = (observer != null) ? observer.getClass().getSimpleName() : null;
    return Utils.format("Pipeline[stages='{}' runner='{}' observer='{}']", instances, runner.getClass().getSimpleName(),
                        observerName);
  }

}
