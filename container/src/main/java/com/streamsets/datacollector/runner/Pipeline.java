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
package com.streamsets.datacollector.runner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.antennadoctor.AntennaDoctor;
import com.streamsets.datacollector.antennadoctor.engine.context.AntennaDoctorStageContext;
import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.InterceptorBean;
import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.creation.PipelineStageBeans;
import com.streamsets.datacollector.creation.ServiceBean;
import com.streamsets.datacollector.creation.StageBean;
import com.streamsets.datacollector.el.JobEL;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.execution.runner.common.PipelineStopReason;
import com.streamsets.datacollector.lineage.LineageEventImpl;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.record.EventRecordImpl;
import com.streamsets.datacollector.runner.production.BadRecordsHandler;
import com.streamsets.datacollector.runner.production.StatsAggregationHandler;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ProtoSource;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.interceptor.InterceptorCreator;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
  private static final String EXECUTION_MODE_CONFIG_KEY = "executionMode";
  private static final String DELIVERY_GUARANTEE_CONFIG_KEY = "deliveryGuarantee";
  public static final String MAX_RUNNERS_CONFIG_KEY = "pipeline.max.runners.count";
  public static final int MAX_RUNNERS_DEFAULT = 50;
  private static final String FRAMEWORK_NAME = "Framework";

  private final StageLibraryTask stageLib;
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
  private final ResourceControlledScheduledExecutor scheduledExecutor;
  private final List<Stage.Info> stageInfos;
  private final UserContext userContext;
  private final List<Map<String, Object>> runnerSharedMaps;
  private final Map<String, Object> runtimeParameters;
  private final long startTime;
  private final BlobStoreTask blobStore;
  private final LineagePublisherTask lineagePublisherTask;
  private final StatsCollector statsCollector;
  private final InterceptorCreatorContextBuilder interceptorContextBuilder;
  private final StageRuntime startEventStage;
  private final StageRuntime stopEventStage;
  private final Map<String, ConnectionConfiguration> connections;
  private boolean stopEventStageInitialized;
  private String controlHubJobId = null;
  private String controlHubJobName = null;

  private Pipeline(
      StageLibraryTask stageLib,
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
      ResourceControlledScheduledExecutor scheduledExecutor,
      List<Stage.Info> stageInfos,
      UserContext userContext,
      List<Map<String, Object>> runnerSharedMaps,
      Map<String, Object> runtimeParameters,
      long startTime,
      BlobStoreTask blobStore,
      LineagePublisherTask lineagePublisherTask,
      StatsCollector statsCollector,
      InterceptorCreatorContextBuilder interceptorCreatorContextBuilder,
      StageRuntime startEventStage,
      StageRuntime stopEventStage,
      Map<String, ConnectionConfiguration> connections
  ) {
    this.stageLib = stageLib;
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
    this.scheduledExecutor = scheduledExecutor;
    this.stageInfos = stageInfos;
    this.runnerSharedMaps = runnerSharedMaps;
    this.userContext = userContext;
    this.runtimeParameters = runtimeParameters;
    this.startTime = startTime;
    this.blobStore = blobStore;
    this.lineagePublisherTask = lineagePublisherTask;
    this.statsCollector = statsCollector;
    this.interceptorContextBuilder = interceptorCreatorContextBuilder;
    this.startEventStage = startEventStage;
    this.stopEventStage = stopEventStage;
    this.connections = connections;
    PipelineBeanCreator.prepareForConnections(configuration, runner.getRuntimeInfo());
    Map<String, Object> parameters = pipelineBean.getConfig().constants;
    if (parameters != null && parameters.get(JobEL.JOB_ID_VAR) != null) {
      this.controlHubJobId = (String) parameters.get(JobEL.JOB_ID_VAR);
      this.controlHubJobName = (String) parameters.get(JobEL.JOB_NAME_VAR);
    }
  }

  public PipelineConfigBean getPipelineConfig() {
    return pipelineBean.getConfig();
  }

  // this is named unfortunately similar to above, but above is used in Transformer so it is hard to rename.
  public PipelineConfiguration getPipelineConf() {
    return pipelineConf;
  }

  @VisibleForTesting
  Pipe getSourcePipe() {
    return originPipe;
  }

  public List<PipeRunner> getRunners() {
    return pipes;
  }

  @VisibleForTesting
  StageRuntime getStartEventStage() {
    return startEventStage;
  }

  @VisibleForTesting
  StageRuntime getStopEventStage() {
    return stopEventStage;
  }
  public int getNumOfRunners() {
    return pipes.size();
  }

  public ProtoSource getSource() {
    return (ProtoSource) originPipe.getStage().getStage();
  }

  public PipelineRunner getRunner() {
    return runner;
  }

  public List<Issue> validateConfigs() throws StageException {
    try {
      return init(false);
    } catch (Throwable throwable) {
      LOG.error("Uncaught error in init: " + throwable, throwable);
      throw Throwables.propagate(throwable);
    } finally {
      try {
        destroy(false, PipelineStopReason.UNUSED);
      } catch (StageException|PipelineRuntimeException e) {
        LOG.error("Exception while destroying() pipeline", e);
      }
    }
  }

  public Map<String, Object> getRuntimeParameters() {
    return runtimeParameters;
  }

  @SuppressWarnings("unchecked")
  public List<Issue> init(boolean productionExecution) {
    PipeContext pipeContext = new PipeContext();
    this.runner.setRuntimeConfiguration(
      pipeContext,
      pipelineConf,
      pipelineBean.getConfig()
    );

    List<Issue> issues = new ArrayList<>();

    // Publish LineageEvent first...
    if(productionExecution) {
      LineageEvent event = createLineageEvent(LineageEventType.START, runner.getRuntimeInfo().getBaseHttpUrl(true));
      lineagePublisherTask.publishEvent(event);
    }

    // Then Error and stats aggregation
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

    // Pipeline lifecycle start event
    if(startEventStage != null) {
      IssueCreator issueCreator = IssueCreator.getStage(startEventStage.getInfo().getInstanceName());
      boolean validationSuccessful = false;

      // Initialize
      try {
        List<Issue> startIssues = startEventStage.init();
        validationSuccessful = startIssues.isEmpty();
        issues.addAll(startIssues);
      } catch (Exception ex) {
        LOG.warn(ContainerError.CONTAINER_0790.getMessage(), ex.toString(), ex);
        issues.add(issueCreator.create(ContainerError.CONTAINER_0790, ex.toString()));
      }

      // Run if in production mode
      try {
        if(productionExecution && validationSuccessful) {
          LOG.info("Processing lifecycle start event with stage");
          runner.runLifecycleEvent(createStartEvent(), startEventStage);
        }
      } catch (Exception ex) {
        LOG.warn(ContainerError.CONTAINER_0791.getMessage(), ex.toString(), ex);
        issues.add(issueCreator.create(ContainerError.CONTAINER_0791, ex.toString()));
      }
    }

    if(stopEventStage != null) {
      IssueCreator issueCreator = IssueCreator.getStage(stopEventStage.getInfo().getInstanceName());

      // Initialize
      try {
        issues.addAll(stopEventStage.init());
        stopEventStageInitialized = issues.isEmpty();
      } catch (Exception ex) {
        LOG.warn(ContainerError.CONTAINER_0790.getMessage(), ex.toString(), ex);
        issues.add(issueCreator.create(ContainerError.CONTAINER_0790, ex.toString()));
      }
    }

    // Initialize origin
    issues.addAll(initPipe(originPipe, pipeContext));
    int runnerCount = 1;

    // If it's a push source, we need to initialize the remaining source-less pipes
    if(originPipe.getStage().getStage() instanceof PushSource) {
      Preconditions.checkArgument(pipes.size() == 1, "There are already more runners then expected");

      // Effective number of runners - either number of source threads or predefined value from user, whatever is *less*
      runnerCount = ((PushSource) originPipe.getStage().getStage()).getNumberOfThreads();
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
              stageLib,
              pipelineBean.getPipelineStageBeans(),
              interceptorContextBuilder,
              originPipe.getStage().getConstants(),
              userContext.getUser(),
              connections,
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
              stageLib,
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
              scheduledExecutor,
              runnerSharedMaps,
              startTime,
              blobStore,
              lineagePublisherTask,
              statsCollector
            ));
          }
        } catch (PipelineRuntimeException e) {
          LOG.error("Can't create additional source-less pipeline runner number {}: {}", runnerCount, e.toString(), e);
          issues.add(IssueCreator.getPipeline().create(ContainerError.CONTAINER_0704, e.toString()));
        }
      }
    }

    // Initialize all source-less pipeline runners
    final int finalRunnerCount = runnerCount;
    for(PipeRunner pipeRunner: pipes) {
      pipeRunner.forEach("Starting", pipe -> {
        ((StageContext)pipe.getStage().getContext()).setPipelineFinisherDelegate((PipelineFinisherDelegate)runner);
        ((StageContext)pipe.getStage().getContext()).setRunnerCount(finalRunnerCount);
        issues.addAll(initPipe(pipe, pipeContext));
      });
    }
    ((StageContext)originPipe.getStage().getContext()).setRunnerCount(runnerCount);
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

  public void destroy(boolean productionExecution, PipelineStopReason stopReason) throws StageException, PipelineRuntimeException {
    LOG.info("Destroying pipeline with reason={}", stopReason.name());

    // Ensure that all stages are properly stopped. This method is usually called by the framework when a pipeline
    // stops properly (in order to force the pipeline to stop). However if the pipeline is failing (random runtime
    // exception), then we need to make sure of that ourselves here.
    stop();

    Throwable exception = null;

    try {
      runner.destroy(originPipe, pipes, badRecordsHandler, statsAggregationHandler);
    } catch (StageException|PipelineRuntimeException ex) {
      String msg = Utils.format("Exception thrown in destroy phase: {}", ex.getMessage());
      LOG.error(msg, ex);
      if(exception == null) {
        exception = ex;
      }
      stopReason = PipelineStopReason.FAILURE;
    }

    // Lifecycle event handling
    if(startEventStage != null) {
      try {
        startEventStage.destroy(null, null, null);
      } catch (Exception ex) {
        String msg = Utils.format("Exception thrown during pipeline start event handler destroy: {}", ex);
        LOG.error(msg, ex);
        if(exception == null) {
          exception = ex;
        }
        stopReason = PipelineStopReason.FAILURE;
      }
    }

    try {
      badRecordsHandler.destroy();
    } catch (Exception ex) {
      String msg = Utils.format("Exception thrown during bad record handler destroy: {}", ex);
      LOG.error(msg, ex);
      if(exception == null) {
        exception = ex;
      }
      stopReason = PipelineStopReason.FAILURE;
    }
    try {
      if (statsAggregationHandler != null) {
        statsAggregationHandler.destroy();
      }
    } catch (Exception ex) {
      String msg = Utils.format("Exception thrown during Stats Aggregator handler destroy: {}", ex);
      LOG.error(msg, ex);
      if(exception == null) {
        exception = ex;
      }
      stopReason = PipelineStopReason.FAILURE;
    }

    if(stopEventStage != null) {
      try {
        if(productionExecution && stopEventStageInitialized) {
          LOG.info("Processing lifecycle stop event");
          runner.runLifecycleEvent(createStopEvent(stopReason), stopEventStage);
        }
      } catch (Exception ex) {
        String msg = Utils.format("Can't execute pipeline stop stage: {}", ex);
        LOG.error(msg, ex);
        exception = new PipelineRuntimeException(ContainerError.CONTAINER_0791, ex.toString());
      }

      // Destroy
      try {
        stopEventStage.destroy(null, null, null);
      } catch (Exception ex) {
        String msg = Utils.format("Exception thrown during pipeline stop event handler destroy: {}", ex);
        LOG.error(msg, ex);
        if(exception == null) {
          exception = ex;
        }
      }
    }

    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdown();
    }

    if(productionExecution) {
      LineageEvent event = createLineageEvent(LineageEventType.STOP, runner.getRuntimeInfo().getBaseHttpUrl(true));
      event.getProperties().put("Pipeline_Stop_Reason", stopReason.name());
      lineagePublisherTask.publishEvent(event);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Pipeline finished destroying with final reason={}", stopReason.name());
    }
    // Propagate exception if it was thrown
    if(exception != null) {
      Throwables.propagateIfInstanceOf(exception, StageException.class);
      Throwables.propagateIfInstanceOf(exception, PipelineRuntimeException.class);
      throw new RuntimeException(exception);
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
      pipeRunner.forEach("Destroying", p -> ((StageContext)p.getStage().getContext()).setStop(true));
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
    private final long startTime;
    private final BlobStoreTask blobStore;
    private final LineagePublisherTask lineagePublisherTask;
    private final InterceptorCreatorContextBuilder interceptorCreatorContextBuilder;
    private final StatsCollector statsCollector;
    private Observer observer;
    private final ResourceControlledScheduledExecutor scheduledExecutor = null;
    private final Map<String, ConnectionConfiguration> connections;
    private List<Issue> errors;

    public Builder(
        StageLibraryTask stageLib,
        Configuration configuration,
        RuntimeInfo runtimeInfo,
        String name,
        String pipelineName,
        String rev,
        UserContext userContext,
        PipelineConfiguration pipelineConf,
        long startTime,
        BlobStoreTask blobStore,
        LineagePublisherTask lineagePublisherTask,
        StatsCollector statsCollector,
        List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
        Map<String, ConnectionConfiguration> connections
    ) {
      this.stageLib = stageLib;
      this.name = name;
      this.pipelineName = pipelineName;
      this.rev = rev;
      this.userContext = userContext;
      this.configuration = configuration;
      this.pipelineConf = pipelineConf;
      this.errors = Collections.emptyList();
      this.startTime = startTime;
      this.blobStore = blobStore;
      this.lineagePublisherTask = lineagePublisherTask;
      this.statsCollector = statsCollector;
      this.interceptorCreatorContextBuilder = new InterceptorCreatorContextBuilder(
        blobStore,
        configuration,
        interceptorConfs
      );
      PipelineBeanCreator.prepareForConnections(configuration, runtimeInfo);
      this.connections = connections;
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
          interceptorCreatorContextBuilder,
          errors,
          runtimeParameters,
          userContext.getUser(),
          connections
      );
      StageRuntime errorStage;
      StageRuntime statsAggregator;
      List<PipeRunner> pipes = new ArrayList<>();
      List<Map<String, Object>> runnerSharedMaps = new ArrayList<>();
      if (pipelineBean != null) {
        // Origin runtime and pipe
        StageRuntime originRuntime = createAndInitializeStageRuntime(
          stageLib,
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
          startTime,
          blobStore,
          lineagePublisherTask,
          false,
          statsCollector
        );

        SourcePipe originPipe = createOriginPipe(originRuntime, runner);

        // Generate shared maps for all runners
        for(StageBean ignore : pipelineBean.getPipelineStageBeans().getStages()) {
          runnerSharedMaps.add(new ConcurrentHashMap<>());
        }

        // Generate runtime and pipe for the first source-less pipeline runner
        pipes.add(createSourceLessRunner(
          stageLib,
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
          scheduledExecutor,
          runnerSharedMaps,
          startTime,
          blobStore,
          lineagePublisherTask,
          statsCollector
        ));

        // Error stage handling
        BadRecordsHandler badRecordsHandler = null;
        if (pipelineBean.getErrorStage() != null) {
          errorStage = createAndInitializeStageRuntime(
              stageLib,
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
              startTime,
              blobStore,
              lineagePublisherTask,
              true,
              statsCollector
          );
          badRecordsHandler = new BadRecordsHandler(
              pipelineBean.getConfig().errorRecordPolicy,
              runner.getRuntimeInfo(),
              errorStage,
              pipelineName
          );
        }

        // And finally Stats aggregation
        StatsAggregationHandler statsAggregationHandler = null;
        if (pipelineBean.getStatsAggregatorStage() != null) {
          statsAggregator = createAndInitializeStageRuntime(
            stageLib,
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
            startTime,
            blobStore,
            lineagePublisherTask,
            false,
            statsCollector
          );

          statsAggregationHandler = new StatsAggregationHandler(statsAggregator);
        }

        // Event stages - we currently support either no or one event stage (which is enforced by various validations)
        Preconditions.checkArgument(
          pipelineBean.getStartEventStages().size() < 2,
          "Unsupported number of start event stages: " + pipelineBean.getStartEventStages().size()
        );
        Preconditions.checkArgument(
          pipelineBean.getStopEventStages().size() < 2,
          "Unsupported number of stop event stages: " + pipelineBean.getStopEventStages().size()
        );
        StageRuntime startEventStageRuntime = null;
        StageRuntime stopEventStageRuntime = null;
        if(pipelineBean.getStartEventStages().size() == 1) {
          startEventStageRuntime = createAndInitializeStageRuntime(
            stageLib,
            pipelineConf,
            pipelineBean,
            pipelineBean.getStartEventStages().get(0),
            runner,
            stageInfos,
            false,
            pipelineName,
            rev,
            userContext,
            configuration,
            0,
            new ConcurrentHashMap<>(),
            startTime,
            blobStore,
            lineagePublisherTask,
            false,
            statsCollector
          );
        }
        if(pipelineBean.getStopEventStages().size() == 1) {
          stopEventStageRuntime = createAndInitializeStageRuntime(
            stageLib,
            pipelineConf,
            pipelineBean,
            pipelineBean.getStopEventStages().get(0),
            runner,
            stageInfos,
            false,
            pipelineName,
            rev,
            userContext,
            configuration,
            0,
            new ConcurrentHashMap<>(),
            startTime,
            blobStore,
            lineagePublisherTask,
            false,
            statsCollector
          );
        }

        try {
          pipeline = new Pipeline(
            stageLib,
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
            scheduledExecutor,
            stageInfos,
            userContext,
            runnerSharedMaps,
            runtimeParameters,
            startTime,
            blobStore,
            lineagePublisherTask,
            statsCollector,
            interceptorCreatorContextBuilder,
            startEventStageRuntime,
            stopEventStageRuntime,
            connections
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
        originRuntime,
        laneResolver.getStageInputLanes(0),
        laneResolver.getStageOutputLanes(0),
        laneResolver.getStageEventLanes(0),
        statsCollector,
        runner.getMetricRegistryJson()
      );
    }

  }

  private static PipeRunner createSourceLessRunner(
    StageLibraryTask stageLib,
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
    ResourceControlledScheduledExecutor scheduledExecutor,
    List<Map<String, Object>> sharedRunnerMaps,
    long startTime,
    BlobStoreTask blobStore,
    LineagePublisherTask lineagePublisherTask,
    StatsCollector statsCollector
  ) throws PipelineRuntimeException {
    Preconditions.checkArgument(beans.size() == sharedRunnerMaps.size(),
      Utils.format("New runner have different number of states then original one! ({} != {})", beans.size(), sharedRunnerMaps.size()));

    List<StageRuntime> stages = new ArrayList<>(1 + beans.size());
    stages.add(originRuntime);

    for(int i = 0; i <  beans.getStages().size(); i ++) {
      StageBean stageBean = beans.get(i);
      Map<String, Object> sharedRunnerMap = sharedRunnerMaps.get(i);

      stages.add(createAndInitializeStageRuntime(
        stageLib,
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
        startTime,
        blobStore,
        lineagePublisherTask,
        false,
        statsCollector
      ));
    }

    return new PipeRunner(
      pipelineName,
      rev,
      runnerId,
      runner.getMetrics(),
      createPipes(
        pipelineName,
        rev,
        stages,
        runner,
        observer
      )
    );
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
    List<StageRuntime> stages,
    PipelineRunner runner,
    Observer observer
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
          pipe = new StagePipe(
            pipelineName,
            rev,
            stage,
            laneResolver.getStageInputLanes(idx),
            laneResolver.getStageOutputLanes(idx),
            laneResolver.getStageEventLanes(idx),
            runner.getMetricRegistryJson()
          );
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
          pipe = new StagePipe(
            pipelineName,
            rev,
            stage,
            laneResolver.getStageInputLanes(idx),
            laneResolver.getStageOutputLanes(idx),
            laneResolver.getStageEventLanes(idx),
            runner.getMetricRegistryJson()
          );
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

  public static StageRuntime createAndInitializeStageRuntime(
    StageLibraryTask stageLib,
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
    long startTime,
    BlobStoreTask blobStore,
    LineagePublisherTask lineagePublisherTask,
    boolean isErrorStage,
    StatsCollector statsCollector
  ) {
    EmailSender emailSender = new EmailSender(configuration);

    // Create runtime structures for all services of this stage
    Map<Class, ServiceRuntime> services = new HashMap<>();
    for(ServiceBean serviceBean: stageBean.getServices()) {
      ServiceRuntime runtime = new ServiceRuntime(pipelineBean, serviceBean);

      runtime.setContext(new ServiceContext(
        configuration,
        pipelineBean.getConfig().constants,
        emailSender,
        pipelineRunner.getMetrics(),
        pipelineName,
        pipelineRev,
        runnerId,
        stageBean.getConfiguration().getInstanceName(),
        runtime,
        serviceBean.getDefinition().getClassName(),
        pipelineRunner.getRuntimeInfo().getResourcesDir()
      ));

      services.put(serviceBean.getDefinition().getProvides(), runtime);
    }

    // Properly wrap the interceptors
    List<InterceptorRuntime> preInterceptors = new ArrayList<>();
    for(InterceptorBean interceptorBean : stageBean.getPreInterceptors()) {
      InterceptorRuntime interceptorRuntime = new InterceptorRuntime(
        InterceptorCreator.InterceptorType.PRE_STAGE,
        interceptorBean
      );

      interceptorRuntime.setContext(new InterceptorContext(
        InterceptorCreator.InterceptorType.PRE_STAGE,
        blobStore,
        configuration,
        stageBean.getConfiguration().getInstanceName(),
        interceptorBean.getMetricName(),
        stageLib,
        pipelineName,
        pipelineConfiguration.getTitle(),
        pipelineRev,
        pipelineRunner.getRuntimeInfo().getId(),
        pipelineRunner.isPreview(),
        userContext,
        pipelineRunner.getMetrics(),
        getExecutionMode(pipelineConfiguration),
        getDeliveryGuarantee(pipelineConfiguration),
        pipelineRunner.getBuildInfo(),
        pipelineRunner.getRuntimeInfo(),
        emailSender,
        startTime,
        new LineagePublisherDelegator.TaskDelegator(lineagePublisherTask)
      ));

      preInterceptors.add(interceptorRuntime);
    }
    List<InterceptorRuntime> postInterceptors = new ArrayList<>();
    for(InterceptorBean interceptorBean : stageBean.getPostInterceptors()) {
      InterceptorRuntime interceptorRuntime = new InterceptorRuntime(
        InterceptorCreator.InterceptorType.POST_STAGE,
        interceptorBean
      );

      interceptorRuntime.setContext(new InterceptorContext(
        InterceptorCreator.InterceptorType.POST_STAGE,
        blobStore,
        configuration,
        stageBean.getConfiguration().getInstanceName(),
        interceptorBean.getMetricName(),
        stageLib,
        pipelineName,
        pipelineConfiguration.getTitle(),
        pipelineRev,
        pipelineRunner.getRuntimeInfo().getId(),
        pipelineRunner.isPreview(),
        userContext,
        pipelineRunner.getMetrics(),
        getExecutionMode(pipelineConfiguration),
        getDeliveryGuarantee(pipelineConfiguration),
        pipelineRunner.getBuildInfo(),
        pipelineRunner.getRuntimeInfo(),
        emailSender,
        startTime,
        new LineagePublisherDelegator.TaskDelegator(lineagePublisherTask)
      ));

      postInterceptors.add(interceptorRuntime);
    }

    AntennaDoctor antennaDoctor = AntennaDoctor.getInstance();
    AntennaDoctorStageContext antennaDoctorContext = null;
    if(antennaDoctor != null) {
      antennaDoctorContext = antennaDoctor.getContext().forStage(
          stageBean.getDefinition(),
          stageBean.getConfiguration(),
          pipelineConfiguration
      );
    }

    // Create StageRuntime itself
    StageRuntime stageRuntime = new StageRuntime(
      pipelineBean,
      stageBean,
      services.values(),
      preInterceptors,
      postInterceptors,
      antennaDoctor,
      antennaDoctorContext
    );

    // Add it to Info array
    if (addToStageInfos) {
      stageInfos.add(stageRuntime.getInfo());
    }


    // And finally create StageContext
    stageRuntime.setContext(
      new StageContext(
        pipelineName,
        pipelineConfiguration.getTitle(),
        pipelineConfiguration.getDescription(),
        pipelineRev,
        pipelineConfiguration.getMetadata(),
        Collections.unmodifiableList(stageInfos),
        userContext,
        stageRuntime.getDefinition().getType(),
        runnerId,
        pipelineRunner.isPreview(),
        pipelineRunner.getMetrics(),
        stageRuntime.getDefinition().getConfigDefinitions(),
        stageRuntime.getOnRecordError(),
        stageRuntime.getConfiguration().getOutputLanes(),
        stageRuntime.getConstants(),
        stageRuntime.getInfo(),
        getExecutionMode(pipelineConfiguration),
        getDeliveryGuarantee(pipelineConfiguration),
        pipelineRunner.getBuildInfo(),
        pipelineRunner.getRuntimeInfo(),
        emailSender,
        configuration,
        runnerSharedMap,
        startTime,
        new LineagePublisherDelegator.TaskDelegator(lineagePublisherTask),
        services,
        isErrorStage,
        antennaDoctor,
        antennaDoctorContext,
        statsCollector,
        stageRuntime.getDefinition().getRecordsByRef()
      )
    );

    return stageRuntime;
  }

  @Override
  public String toString() {
    Set<String> instances = new LinkedHashSet<>();
    // Describing first runner is sufficient
    pipes.get(0).forEach("", pipe -> instances.add(pipe.getStage().getInfo().getInstanceName()));
    String observerName = (observer != null) ? observer.getClass().getSimpleName() : null;
    return Utils.format(
      "Pipeline[source='{}' stages='{}' runner='{}' observer='{}']",
      originPipe.getStage().getInfo().getInstanceName(),
      instances,
      runner.getClass().getSimpleName(),
      observerName
    );
  }

  /**
   * Create pipeline start event.
   */
  private Record createStartEvent() {
    Preconditions.checkState(startEventStage != null, "Start Event Stage is not set!");
    EventRecord eventRecord = new EventRecordImpl(
      "pipeline-start",
      1,
      startEventStage.getInfo().getInstanceName(),
      "",
      null,
      null
    );

    Map<String, Field> rootField = new LinkedHashMap<>();
    rootField.put("user", Field.create(Field.Type.STRING, userContext.getUser()));
    rootField.put("pipelineId", Field.create(Field.Type.STRING, name));
    rootField.put("pipelineTitle", Field.create(Field.Type.STRING, pipelineConf.getTitle()));
    if (controlHubJobId != null) {
      rootField.put("jobId", Field.create(controlHubJobId));
      rootField.put("jobName", Field.create(controlHubJobName));
    }

    // Pipeline parameters
    Map<String, Field> parameters = new LinkedHashMap<>();
    if(runtimeParameters != null) {
      for (Map.Entry<String, Object> entry : runtimeParameters.entrySet()) {
        parameters.put(
          entry.getKey(),
          Field.create(Field.Type.STRING, entry.getValue().toString())
        );
      }
    }
    rootField.put("parameters", Field.create(parameters));

    eventRecord.set(Field.create(rootField));
    return eventRecord;
  }

  /**
   * Create pipeline stop event.
   */
  private Record createStopEvent(PipelineStopReason stopReason) {
    Preconditions.checkState(stopEventStage != null, "Stop Event Stage is not set!");
    EventRecord eventRecord = new EventRecordImpl(
      "pipeline-stop",
      1,
      stopEventStage.getInfo().getInstanceName(),
      "",
      null,
      null
    );

    Map<String, Field> rootField = new LinkedHashMap<>();
    rootField.put("reason", Field.create(Field.Type.STRING, stopReason.name()));
    rootField.put("pipelineId", Field.create(Field.Type.STRING, name));
    rootField.put("pipelineTitle", Field.create(Field.Type.STRING, pipelineConf.getTitle()));
    if (controlHubJobId != null) {
      rootField.put("jobId", Field.create(controlHubJobId));
      rootField.put("jobName", Field.create(controlHubJobName));
    }

    eventRecord.set(Field.create(rootField));
    return eventRecord;
  }

  private LineageEvent createLineageEvent(LineageEventType type, String partURL) {
    if (!type.isFrameworkOnly()) {
      throw new IllegalArgumentException(Utils.format(ContainerError.CONTAINER_01402.getMessage(), type.getLabel()));
    }

    return new LineageEventImpl(
        type,
        pipelineConf.getTitle(),
        userContext.getUser(),
        startTime,
        pipelineConf.getPipelineId(), // pipelineId generated by SDC
        runner.getRuntimeInfo().getId(),
        partURL + LineageEventImpl.PARTIAL_URL + pipelineConf.getPipelineId(),
        FRAMEWORK_NAME,
        pipelineConf.getDescription(),
        rev, // pipeline version generated by SDC
        pipelineConf.getMetadata(),
        pipelineBean.getConfig().constants
    );
  }

}
