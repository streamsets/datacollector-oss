/**
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
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.memory.MemoryUsageCollectorResourceBundle;
import com.streamsets.datacollector.runner.production.BadRecordsHandler;
import com.streamsets.datacollector.runner.production.StatsAggregationHandler;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.ProtoSource;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
  private static final String EXECUTION_MODE_CONFIG_KEY = "executionMode";
  private static final String DELIVERY_GUARANTEE_CONFIG_KEY = "deliveryGuarantee";
  private static final String MAX_RUNNERS_CONFIG_KEY = "pipeline.max.runners.count";
  private static final int MAX_RUNNERS_DEFAULT = 50;

  private final PipelineBean pipelineBean;
  private final String name;
  private final String rev;
  private final Configuration configuration;
  private final PipelineConfiguration pipelineConf;
  private final SourcePipe originPipe;
  private final List<PipeRunner> pipes;
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
  private final UserContext userContext;
  private final List<Map<String, Object>> runnerSharedMaps;
  private final Map<String, Object> runtimeParameters;
  private final LineagePublisherTask lineagePublisherTask;

  private Pipeline(
      String name,
      String rev,
      Configuration configuration,
      PipelineConfiguration pipelineConf,
      PipelineBean pipelineBean,
      SourcePipe originPipe,
      List<PipeRunner> pipes,
      Observer observer,
      BadRecordsHandler badRecordsHandler,
      PipelineRunner runner,
      ResourceControlledScheduledExecutor scheduledExecutorService,
      StatsAggregationHandler statsAggregationHandler,
      MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle,
      ResourceControlledScheduledExecutor scheduledExecutor,
      List<Stage.Info> stageInfos,
      UserContext userContext,
      List<Map<String, Object>> runnerSharedMaps,
      Map<String, Object> runtimeParameters,
      LineagePublisherTask lineagePublisherTask
  ) {
    this.pipelineBean = pipelineBean;
    this.name = name;
    this.rev = rev;
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
    this.runnerSharedMaps = runnerSharedMaps;
    this.userContext = userContext;
    this.runtimeParameters = runtimeParameters;
    this.lineagePublisherTask = lineagePublisherTask;
  }

  PipelineConfigBean getPipelineConfig() {
    return pipelineBean.getConfig();
  }


  @VisibleForTesting
  Pipe getSourcePipe() {
    return originPipe;
  }

  public List<PipeRunner> getRunners() {
    return pipes;
  }

  public int getNumOfRunners() {
    return pipes.size();
  }

  private boolean calculateShouldStopOnStageError() {
    // Check origin
    StageContext stageContext = originPipe.getStage().getContext();
    if(stageContext.getOnErrorRecord() == OnRecordError.STOP_PIPELINE) {
      return true;
    }

    // Working with only first runner is sufficient here as all runners share the same configuration
    return pipes.get(0).onRecordErrorStopPipeline();
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

  public Map<String, Object> getRuntimeParameters() {
    return runtimeParameters;
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

      // Effective number of runners - either number of source threads or predefined value from user, whatever is *less*
      int runnerCount = ((PushSource) originPipe.getStage().getStage()).getNumberOfThreads();
      int pipelineRunnerCount = pipelineBean.getConfig().maxRunners;
      if (pipelineRunnerCount > 0) {
        runnerCount = Math.min(runnerCount, pipelineRunnerCount);
      }

      // Ensure that it doesn't go over configured threshold
      int sdcRunnerMax = configuration.get(MAX_RUNNERS_CONFIG_KEY, MAX_RUNNERS_DEFAULT);
      boolean createAdditionalRunners = true;
      if (runnerCount > sdcRunnerMax) {
        createAdditionalRunners = false;
        issues.add(IssueCreator.getPipeline().create(ContainerError.CONTAINER_0705, runnerCount, sdcRunnerMax));
      }

      // Unless the request number of runners is invalid, let's create them
      if (createAdditionalRunners) {
        try {
          for (int runnerId = 1; runnerId < runnerCount; runnerId++) {
            List<Issue> localIssues = new ArrayList<>();

            // Create list of Stage beans
            PipelineStageBeans beans = PipelineBeanCreator.get().duplicatePipelineStageBeans(
              pipelineBean.getPipelineStageBeans(),
              originPipe.getStage().getConstants(),
              localIssues
            );

            // If there was an issue creating the beans, don't continue
            if(!localIssues.isEmpty()) {
              issues.addAll(localIssues);

              // To create the beans, we've already got class loaders, so we need to release them (they would leak otherwise
              // as the beans object is not persisted anywhere).
              beans.getStages().forEach(StageBean::releaseClassLoader);

              break;
            }

            // Initialize and convert them to source-less pipeline runner
            pipes.add(createSourceLessRunner(
              name,
              rev,
              configuration,
              pipelineConf,
              runner,
              stageInfos,
              userContext,
              pipelineBean,
              originPipe.getStage(),
              runnerId,
              beans,
              observer,
              memoryUsageCollectorResourceBundle,
              scheduledExecutor,
              runnerSharedMaps,
              lineagePublisherTask
            ));
          }
        } catch (PipelineRuntimeException e) {
          LOG.error("Can't create additional source-less pipeline runner number {}: {}", runnerCount, e.toString(), e);
          issues.add(IssueCreator.getPipeline().create(ContainerError.CONTAINER_0704, e.toString()));
        }
      }
    }

    // Initialize all source-less pipeline runners
    for(PipeRunner pipeRunner: pipes) {
      pipeRunner.forEachNoException(pipe -> {
        ((StageContext)pipe.getStage().getContext()).setPipelineFinisherDelegate((PipelineFinisherDelegate)runner);
        issues.addAll(initPipe(pipe, pipeContext));
      });
    }
    ((StageContext)originPipe.getStage().getContext()).setPipelineFinisherDelegate((PipelineFinisherDelegate)runner);

    return issues;
  }

  private List<Issue> initPipe(Pipe pipe, PipeContext pipeContext) {
    try {
      return pipe.init(pipeContext);
    } catch (Exception ex) {
      String instanceName = pipe.getStage().getConfiguration().getInstanceName();
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
    for(PipeRunner pipeRunner : pipes) {
      pipeRunner.forEachNoException(p -> ((StageContext)p.getStage().getContext()).setStop(true));
    }
  }

  public static class Builder {

    private final StageLibraryTask stageLib;
    private final Configuration configuration;
    private final String name;
    private final String pipelineName;
    private final String rev;
    private final UserContext userContext;
    private final PipelineConfiguration pipelineConf;
    private final LineagePublisherTask lineagePublisherTask;
    private Observer observer;
    private final ResourceControlledScheduledExecutor scheduledExecutor =
        new ResourceControlledScheduledExecutor(0.01f); // consume 1% of a cpu calculating stage memory consumption
    private final MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle =
        new MemoryUsageCollectorResourceBundle();
    private List<Issue> errors;

    public Builder(
        StageLibraryTask stageLib,
        Configuration configuration,
        String name,
        String pipelineName,
        String rev,
        UserContext userContext,
        PipelineConfiguration pipelineConf,
        LineagePublisherTask lineagePublisherTask
    ) {
      this.stageLib = stageLib;
      this.name = name;
      this.pipelineName = pipelineName;
      this.rev = rev;
      this.userContext = userContext;
      this.configuration = configuration;
      this.pipelineConf = pipelineConf;
      this.errors = Collections.emptyList();
      this.lineagePublisherTask = lineagePublisherTask;
    }

    public Builder setObserver(Observer observer) {
      this.observer = observer;
      return this;
    }

    public Pipeline build(PipelineRunner runner) throws PipelineRuntimeException {
      return build(runner, null);
    }

    public Pipeline build(PipelineRunner runner, Map<String, Object> runtimeParameters) throws PipelineRuntimeException {
      Pipeline pipeline = null;
      errors = new ArrayList<>();
      List<Stage.Info> stageInfos = new ArrayList<>();
      PipelineBean pipelineBean = PipelineBeanCreator.get().create(
          true,
          stageLib,
          pipelineConf,
          errors,
          runtimeParameters
      );
      StageRuntime errorStage;
      StageRuntime statsAggregator;
      List<PipeRunner> pipes = new ArrayList<>();
      List<Map<String, Object>> runnerSharedMaps = new ArrayList<>();
      if (pipelineBean != null) {
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
          userContext,
          configuration,
          0,
          new ConcurrentHashMap<>(),
          lineagePublisherTask
        );

        SourcePipe originPipe = createOriginPipe(originRuntime, runner);

        // Generate shared maps for all runners
        for(StageBean ignore : pipelineBean.getPipelineStageBeans().getStages()) {
          runnerSharedMaps.add(new ConcurrentHashMap<>());
        }

        // Generate runtime and pipe for the first source-less pipeline runner
        pipes.add(createSourceLessRunner(
          pipelineName,
          rev,
          configuration,
          pipelineConf,
          runner,
          stageInfos,
          userContext,
          pipelineBean,
          originRuntime,
          0,
          pipelineBean.getPipelineStageBeans(),
          observer,
          memoryUsageCollectorResourceBundle,
          scheduledExecutor,
          runnerSharedMaps,
          lineagePublisherTask
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
          userContext,
          configuration,
          0,
          new ConcurrentHashMap<>(),
          lineagePublisherTask
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
            userContext,
            configuration,
            0,
            new ConcurrentHashMap<>(),
            lineagePublisherTask
          );

          statsAggregationHandler = new StatsAggregationHandler(statsAggregator);
        }

        try {
          pipeline = new Pipeline(
            name,
            rev,
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
            stageInfos,
            userContext,
            runnerSharedMaps,
            runtimeParameters,
            lineagePublisherTask
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

  private static PipeRunner createSourceLessRunner(
    String pipelineName,
    String rev,
    Configuration configuration,
    PipelineConfiguration pipelineConf,
    PipelineRunner runner,
    List<Stage.Info> stageInfos,
    UserContext userContext,
    PipelineBean pipelineBean,
    StageRuntime originRuntime,
    int runnerId,
    PipelineStageBeans beans,
    Observer observer,
    MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle,
    ResourceControlledScheduledExecutor scheduledExecutor,
    List<Map<String, Object>> sharedRunnerMaps,
    LineagePublisherTask lineagePublisherTask
  ) throws PipelineRuntimeException {
    Preconditions.checkArgument(beans.size() == sharedRunnerMaps.size(),
      Utils.format("New runner have different number of states then original one! ({} != {})", beans.size(), sharedRunnerMaps.size()));

    List<StageRuntime> stages = new ArrayList<>(1 + beans.size());
    stages.add(originRuntime);

    for(int i = 0; i <  beans.getStages().size(); i ++) {
      StageBean stageBean = beans.get(i);
      Map<String, Object> sharedRunnerMap = sharedRunnerMaps.get(i);

      stages.add(createAndInitializeStageRuntime(
        pipelineConf,
        pipelineBean,
        stageBean,
        runner,
        stageInfos,
        true,
        pipelineName,
        rev,
        userContext,
        configuration,
        runnerId,
        sharedRunnerMap,
        lineagePublisherTask
      ));
    }

    return new PipeRunner(runnerId, createPipes(
      pipelineName,
      rev,
      configuration,
      stages,
      runner,
      observer,
      memoryUsageCollectorResourceBundle,
      scheduledExecutor
    ));
  }

  private static ExecutionMode getExecutionMode(PipelineConfiguration pipelineConf) {
    Config executionModeConfig = Utils.checkNotNull(pipelineConf.getConfiguration(EXECUTION_MODE_CONFIG_KEY), EXECUTION_MODE_CONFIG_KEY);
    String executionMode = executionModeConfig.getValue().toString();
    Utils.checkState(executionMode != null && !executionMode.isEmpty(), "Execution mode cannot be null or empty");
    return ExecutionMode.valueOf(executionMode);
  }

  private static DeliveryGuarantee getDeliveryGuarantee(PipelineConfiguration pipelineConf) {
    Config deliveryGuaranteeConfig = Utils.checkNotNull(pipelineConf.getConfiguration(DELIVERY_GUARANTEE_CONFIG_KEY), DELIVERY_GUARANTEE_CONFIG_KEY);
    String deliveryGuarantee = deliveryGuaranteeConfig.getValue().toString();
    Utils.checkState(deliveryGuarantee != null && !deliveryGuarantee.isEmpty(), "Delivery guarantee cannot be null or empty");
    return DeliveryGuarantee.valueOf(deliveryGuarantee);
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
    UserContext userContext,
    Configuration configuration,
    int runnerId,
    Map<String, Object> runnerSharedMap,
    LineagePublisherTask lineagePublisherTask
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
        userContext,
        stageRuntime.getDefinition().getType(),
        runnerId,
        pipelineRunner.isPreview(),
        pipelineRunner.getMetrics(),
        stageRuntime,
        pipelineConfiguration.getMemoryLimitConfiguration().getMemoryLimit(),
        getExecutionMode(pipelineConfiguration),
        getDeliveryGuarantee(pipelineConfiguration),
        pipelineRunner.getRuntimeInfo(),
        new EmailSender(configuration),
        configuration,
        runnerSharedMap,
        new LineagePublisherDelegator.TaskDelegator(lineagePublisherTask)
      )
    );

    return stageRuntime;
  }

  @Override
  public String toString() {
    Set<String> instances = new LinkedHashSet<>();
    // Describing first runner is sufficient
    pipes.get(0).forEachNoException(pipe -> instances.add(pipe.getStage().getInfo().getInstanceName()));
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
