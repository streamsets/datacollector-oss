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
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.log.LogConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
  private static final String EXECUTION_MODE_CONFIG_KEY = "executionMode";

  private final PipelineBean pipelineBean;
  private final String name;
  private final String rev;
  private final StageLibraryTask stageLib;
  private final Configuration configuration;
  private final PipelineConfiguration pipelineConf;
  private final SourcePipe originPipe;
  private final List<List<Pipe>> pipes;
  private final PipelineRunner runner;
  private final Observer observer;
  private final BadRecordsHandler badRecordsHandler;
  private final StatsAggregationHandler statsAggregationHandler;
  private final ResourceControlledScheduledExecutor scheduledExecutorService;
  private volatile boolean running;
  private boolean shouldStopOnStageError = false;
  private final MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle;
  private final ResourceControlledScheduledExecutor scheduledExecutor;
  private final List<Stage.Info> stageInfos;

  private Pipeline(
      String name,
      String rev,
      StageLibraryTask stageLib,
      Configuration configuration,
      PipelineConfiguration pipelineConf,
      PipelineBean pipelineBean,
      SourcePipe originPipe,
      List<List<Pipe>> pipes,
      Observer observer,
      BadRecordsHandler badRecordsHandler,
      PipelineRunner runner,
      ResourceControlledScheduledExecutor scheduledExecutorService,
      StatsAggregationHandler statsAggregationHandler,
      MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle,
      ResourceControlledScheduledExecutor scheduledExecutor,
      List<Stage.Info> stageInfos
  ) {
    this.pipelineBean = pipelineBean;
    this.name = name;
    this.rev = rev;
    this.stageLib = stageLib;
    this.configuration = configuration;
    this.pipelineConf = pipelineConf;
    this.originPipe = originPipe;
    this.pipes = pipes;
    this.observer = observer;
    this.badRecordsHandler = badRecordsHandler;
    this.runner = runner;
    this.scheduledExecutorService = scheduledExecutorService;
    this.running = false;
    this.statsAggregationHandler = statsAggregationHandler;
    this.shouldStopOnStageError = calculateShouldStopOnStageError();
    this.memoryUsageCollectorResourceBundle = memoryUsageCollectorResourceBundle;
    this.scheduledExecutor = scheduledExecutor;
    this.stageInfos = stageInfos;
  }

  PipelineConfigBean getPipelineConfig() {
    return pipelineBean.getConfig();
  }


  @VisibleForTesting
  Pipe getSourcePipe() {
    return originPipe;
  }

  @VisibleForTesting
  List<List<Pipe>> getRunners() {
    return pipes;
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
    this.runner.setPipelineConfiguration(pipelineConf);
    this.runner.setPipeContext(pipeContext);

    List<Issue> issues = new ArrayList<>();

    // Error and stats aggregation first
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

    // Initialize origin
    issues.addAll(initPipe(originPipe, pipeContext));

    // If it's a push source, we need to initialize the remaining source-less pipes
    if(originPipe.getStage().getStage() instanceof PushSource) {
      Preconditions.checkArgument(pipes.size() == 1, "There are already more runners then expected");

      // TODO: SDC-4728: Add option Maximal number of threads to pipeline config
      int runnerCount = ((PushSource)originPipe.getStage().getStage()).getNumberOfThreads();
      try {
        for (int runnerId = 1; runnerId < runnerCount; runnerId++) {
          // Create list of Stage beans
          PipelineStageBeans beans = PipelineBeanCreator.get().createPipelineStageBeans(
            true,
            stageLib,
            pipelineConf.getStages().subList(1, pipelineConf.getStages().size()),
            originPipe.getStage().getConstants(),
            issues
          );

          // Initialize and convert them to source-less pipeline runner
          pipes.add(createSourceLessRunner(
            name,
            rev,
            configuration,
            pipelineConf,
            runner,
            stageInfos,
            pipelineBean,
            originPipe.getStage(),
            runnerId,
            beans,
            observer,
            memoryUsageCollectorResourceBundle,
            scheduledExecutor
          ));
        }
      } catch (PipelineRuntimeException e) {
        LOG.error("Can't create additional source-less pipeline runner number {}: {}", runnerCount, e.toString(), e);
        issues.add(IssueCreator.getPipeline().create(ContainerError.CONTAINER_0704, e.toString()));
      }
    }

    // Initialize all source-less pipeline runners
    int runnerId = 0;
    for(List<Pipe> runnerPipes: pipes) {
      MDC.put(LogConstants.RUNNER, String.valueOf(runnerId++));
      for(Pipe pipe : runnerPipes) {
        issues.addAll(initPipe(pipe, pipeContext));
      }
    }
    MDC.put(LogConstants.RUNNER, "");
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
      List<Stage.Info> stageInfos = new ArrayList<>();
      PipelineBean pipelineBean = PipelineBeanCreator.get().create(true, stageLib, pipelineConf, errors);
      StageRuntime errorStage;
      StageRuntime statsAggregator;
      List<List<Pipe>> pipes = new ArrayList<>();
      if (pipelineBean != null) {
        Preconditions.checkArgument(!pipelineBean.getPipelineStageBeans().isEmpty(), "At least one instance of pipeline must exist!");

        // Origin runtime and pipe
        StageRuntime originRuntime = createAndInitializeStageRuntime(
          pipelineConf,
          pipelineBean,
          pipelineBean.getOrigin(),
          runner,
          stageInfos,
          true,
          pipelineName,
          rev,
          configuration,
          0
        );

        SourcePipe originPipe = createOriginPipe(originRuntime, runner);

        // Generate runtime and pipe for the first source-less pipeline runner
        Preconditions.checkArgument(pipelineBean.getPipelineStageBeans().size() == 1, "There are already more pipeline stage beans then expected");
        pipes.add(createSourceLessRunner(
          pipelineName,
          rev,
          configuration,
          pipelineConf,
          runner,
          stageInfos,
          pipelineBean,
          originRuntime,
          0,
          pipelineBean.getPipelineStageBeans().get(0),
          observer,
          memoryUsageCollectorResourceBundle,
          scheduledExecutor
        ));

        // Error stage handling
        errorStage = createAndInitializeStageRuntime(
          pipelineConf,
          pipelineBean,
          pipelineBean.getErrorStage(),
          runner,
          stageInfos,
          false,
          pipelineName,
          rev,
          configuration,
          0
        );
        BadRecordsHandler badRecordsHandler = new BadRecordsHandler(errorStage);

        // And finally Stats aggregation
        StatsAggregationHandler statsAggregationHandler = null;
        if (pipelineBean.getStatsAggregatorStage() != null) {
          statsAggregator = createAndInitializeStageRuntime(
            pipelineConf,
            pipelineBean,
            pipelineBean.getStatsAggregatorStage(),
            runner,
            stageInfos,
            false,
            pipelineName,
            rev,
            configuration,
            0
          );

          statsAggregationHandler = new StatsAggregationHandler(statsAggregator);
        }

        try {
          pipeline = new Pipeline(
            name,
            rev,
            stageLib,
            configuration,
            pipelineConf,
            pipelineBean,
            originPipe,
            pipes,
            observer,
            badRecordsHandler,
            runner,
            scheduledExecutor,
            statsAggregationHandler,
            memoryUsageCollectorResourceBundle,
            scheduledExecutor,
            stageInfos
          );
        } catch (Exception e) {
          String msg = "Can't instantiate pipeline: " + e;
          LOG.error(msg, e);
          throw new PipelineRuntimeException(ContainerError.CONTAINER_0151, msg, e);
        }
      }
      return pipeline;
    }

    public List<Issue> getIssues() {
      return errors;
    }

    private SourcePipe createOriginPipe(StageRuntime originRuntime, PipelineRunner runner) {
      LaneResolver laneResolver = new LaneResolver(ImmutableList.of(originRuntime));
      return new SourcePipe(
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

  }

  private static List<Pipe> createSourceLessRunner(
    String pipelineName,
    String rev,
    Configuration configuration,
    PipelineConfiguration pipelineConf,
    PipelineRunner runner,
    List<Stage.Info> stageInfos,
    PipelineBean pipelineBean,
    StageRuntime originRuntime,
    int runnerId,
    PipelineStageBeans beans,
    Observer observer,
    MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle,
    ResourceControlledScheduledExecutor scheduledExecutor
  ) throws PipelineRuntimeException {
    List<StageRuntime> stages = new ArrayList<>(1 + beans.size());
    stages.add(originRuntime);
    for(StageBean stageBean : beans.getStages()) {
      stages.add(createAndInitializeStageRuntime(
        pipelineConf,
        pipelineBean,
        stageBean,
        runner,
        stageInfos,
        true,
        pipelineName,
        rev,
        configuration,
        runnerId
      ));
    }

    return createPipes(
      pipelineName,
      rev,
      configuration,
      stages,
      runner,
      observer,
      memoryUsageCollectorResourceBundle,
      scheduledExecutor
    );
  }

  private static ExecutionMode getExecutionMode(PipelineConfiguration pipelineConf) {
    Config executionModeConfig = Utils.checkNotNull(pipelineConf.getConfiguration(EXECUTION_MODE_CONFIG_KEY), EXECUTION_MODE_CONFIG_KEY);
    String executionMode = executionModeConfig.getValue().toString();
    Utils.checkState(executionMode != null && !executionMode.isEmpty(), "Execution mode cannot be null or empty");
    return ExecutionMode.valueOf(executionMode);
  }

  private static List<Pipe> createPipes(
    String pipelineName,
    String rev,
    Configuration configuration,
    List<StageRuntime> stages,
    PipelineRunner runner,
    Observer observer,
    MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle,
    ResourceControlledScheduledExecutor scheduledExecutor
  ) throws PipelineRuntimeException {
    LaneResolver laneResolver = new LaneResolver(stages);
    ImmutableList.Builder<Pipe> pipesBuilder = ImmutableList.builder();

    int idx = -1;
    for(StageRuntime stage : stages) {
      idx++;
      Pipe pipe;
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

  private static StageRuntime createAndInitializeStageRuntime(
    PipelineConfiguration pipelineConfiguration,
    PipelineBean pipelineBean,
    StageBean stageBean,
    PipelineRunner pipelineRunner,
    List<Stage.Info> stageInfos,
    boolean addToStageInfos,
    String pipelineName,
    String pipelineRev,
    Configuration configuration,
    int runnerId
  ) {
    // Create StageRuntime itself
    StageRuntime stageRuntime = new StageRuntime(pipelineBean, stageBean);

    // Add it to Info array
    if (addToStageInfos) {
      stageInfos.add(stageRuntime.getInfo());
    }

    // And finally create StageContext
    stageRuntime.setContext(
      new StageContext(
        pipelineName,
        pipelineRev,
        Collections.unmodifiableList(stageInfos),
        stageRuntime.getDefinition().getType(),
        runnerId,
        pipelineRunner.isPreview(),
        pipelineRunner.getMetrics(),
        stageRuntime,
        pipelineConfiguration.getMemoryLimitConfiguration().getMemoryLimit(),
        getExecutionMode(pipelineConfiguration),
        pipelineRunner.getRuntimeInfo(),
        new EmailSender(configuration),
        configuration
      )
    );

    return stageRuntime;
  }

  @Override
  public String toString() {
    Set<String> instances = new LinkedHashSet<>();
    // Describing first runner is sufficient
    for (Pipe pipe : getRunners().get(0)) {
      instances.add(pipe.getStage().getInfo().getInstanceName());
    }
    String observerName = (observer != null) ? observer.getClass().getSimpleName() : null;
    return Utils.format(
      "Pipeline[source='{}' stages='{}' runner='{}' observer='{}']",
      originPipe.getStage().getInfo().getInstanceName(),
      instances,
      runner.getClass().getSimpleName(),
      observerName
    );
  }

}
