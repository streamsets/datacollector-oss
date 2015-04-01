/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.memory.MemoryUsageCollectorResourceBundle;
import com.streamsets.pipeline.runner.production.BadRecordsHandler;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.validation.StageIssue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class Pipeline {
  private final Pipe[] pipes;
  private final PipelineRunner runner;
  private final Observer observer;
  private final BadRecordsHandler badRecordsHandler;
  private final ResourceControlledScheduledExecutor scheduledExecutorService;

  private Pipeline(Pipe[] pipes, Observer observer, BadRecordsHandler badRecordsHandler, PipelineRunner runner,
                   ResourceControlledScheduledExecutor scheduledExecutorService) {
    this.pipes = pipes;
    this.observer = observer;
    this.badRecordsHandler = badRecordsHandler;
    this.runner = runner;
    this.scheduledExecutorService = scheduledExecutorService;
  }

  @VisibleForTesting
  Pipe[] getPipes() {
    return pipes;
  }

  public Source getSource() {
    return (Source) pipes[0].getStage().getStage();
  }

  public PipelineRunner getRunner() {
    return runner;
  }

  public List<StageIssue> validateConfigs() throws StageException {
    List<StageIssue> configIssues = new ArrayList<>();
    configIssues.addAll(badRecordsHandler.validate());
    for (Pipe pipe : pipes) {
      configIssues.addAll(pipe.validateConfigs());
    }
    return configIssues;
  }

  @SuppressWarnings("unchecked")
  public void init() throws StageException {
    PipeContext pipeContext = new PipeContext();
    int idx = 0;
    try {
      for (; idx < pipes.length; idx++) {
        pipes[idx].init(pipeContext);
      }
      badRecordsHandler.init();
    } catch (StageException|RuntimeException ex) {
      destroy(idx);
      throw ex;
    }
  }

  private void destroy(int idx) {
    if (idx == pipes.length) {
      badRecordsHandler.destroy();
    }
    for (idx--; idx >=0; idx--) {
      try {
        pipes[idx].destroy();
      } catch (RuntimeException ex) {
        //LOG WARN
      }
    }
  }

  public void destroy() {
    destroy(pipes.length);
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdown();
    }
  }

  public void run() throws StageException, PipelineRuntimeException {
    runner.setObserver(observer);
    runner.run(pipes, badRecordsHandler);
  }

  public void run(List<StageOutput> stageOutputsToOverride) throws StageException, PipelineRuntimeException {
    runner.setObserver(observer);
    runner.run(pipes, badRecordsHandler, stageOutputsToOverride);
  }

  public static class Builder {
    private final StageLibraryTask stageLib;
    private final String name;
    private final PipelineConfiguration pipelineConf;
    private Observer observer;
    private final ResourceControlledScheduledExecutor scheduledExecutor =
      new ResourceControlledScheduledExecutor(0.01f); // consume 1% of a cpu calculating stage memory consumption
    private final MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle =
      new MemoryUsageCollectorResourceBundle();


    public Builder(StageLibraryTask stageLib, String name, PipelineConfiguration pipelineConf) {
      this.stageLib = stageLib;
      this.name = name;
      this.pipelineConf = pipelineConf;
    }
    public Builder setObserver(Observer observer) {
      this.observer = observer;
      return this;
    }

    public Pipeline build(PipelineRunner runner) throws PipelineRuntimeException {
      StageRuntime.Builder builder = new StageRuntime.Builder(stageLib, name, pipelineConf);
      StageRuntime[] stages = builder.build();
      StageRuntime errorStage = builder.buildErrorStage(pipelineConf);
      setStagesContext(stages, errorStage, runner);
      Pipe[] pipes = createPipes(stages);
      BadRecordsHandler badRecordsHandler = new BadRecordsHandler(errorStage);
      try {
        return new Pipeline(pipes, observer, badRecordsHandler, runner, scheduledExecutor);
      } catch (Exception e) {
        String msg = "Could not create memory usage collector: " + e;
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0151, msg, e);
      }
    }

    private void setStagesContext(StageRuntime[] stages, StageRuntime errorStage, PipelineRunner runner) {
      List<Stage.Info> infos = new ArrayList<>(stages.length);
      List<Stage.Info> infosUnmodifiable = Collections.unmodifiableList(infos);
      for (StageRuntime stage : stages) {
        infos.add(stage.getInfo());
        stage.setContext(new StageContext(infosUnmodifiable, stage.getDefinition().getType(), runner.isPreview(),
                                          runner.getMetrics(), stage));
      }
      errorStage.setContext(new StageContext(infosUnmodifiable, errorStage.getDefinition().getType(), runner.isPreview(),
                                             runner.getMetrics(), errorStage));
    }

    private Pipe[] createPipes(StageRuntime[] stages) throws PipelineRuntimeException {
      LaneResolver laneResolver = new LaneResolver(stages);
      List<Pipe> pipes = new ArrayList<>(stages.length * 3);
      for (int idx = 0; idx < stages.length; idx++) {
        Pipe pipe;
        StageRuntime stage = stages[idx];
        switch (stage.getDefinition().getType()) {
          case SOURCE:
            pipe = new StagePipe(stage, laneResolver.getStageInputLanes(idx), laneResolver.getStageOutputLanes(idx),
              scheduledExecutor, memoryUsageCollectorResourceBundle);
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
            pipe = new StagePipe(stage, laneResolver.getStageInputLanes(idx),
                                 laneResolver.getStageOutputLanes(idx), scheduledExecutor,
              memoryUsageCollectorResourceBundle);
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
            pipe = new StagePipe(stage, laneResolver.getStageInputLanes(idx), laneResolver.getStageOutputLanes(idx),
              scheduledExecutor, memoryUsageCollectorResourceBundle);
            pipes.add(pipe);
            break;
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
