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
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
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

  private Pipeline(Pipe[] pipes, Observer observer, PipelineRunner runner) {
    this.pipes = pipes;
    this.observer = observer;
    this.runner = runner;
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

  public void configure(Configuration conf) {
    if (observer != null) {
      observer.configure(conf);
    }
  }

  public List<StageIssue> validateConfigs() {
    List<StageIssue> configIssues = new ArrayList<>();
    for (Pipe pipe : pipes) {
      configIssues.addAll(pipe.validateConfigs());
    }
    return configIssues;
  }

  @SuppressWarnings("unchecked")
  public void init() throws StageException {
    int idx = 0;
    try {
      for (; idx < pipes.length; idx++) {
        pipes[idx].init();
      }
    } catch (StageException|RuntimeException ex) {
      destroy(idx);
      throw ex;
    }
  }

  private void destroy(int idx) {
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
  }

  public void run() throws StageException, PipelineRuntimeException {
    runner.setObserver(observer);
    runner.run(pipes);
  }

  public static class Builder {
    private final StageLibraryTask stageLib;
    private final String name;
    private final PipelineConfiguration pipelineConf;
    private Observer observer;

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
      StageRuntime[] stages = new StageRuntime.Builder(stageLib, name, pipelineConf).build();
      setStagesContext(stages, runner);
      Pipe[] pipes = createPipes(stages);
      return new Pipeline(pipes, observer, runner);
    }

    private void setStagesContext(StageRuntime[] stages, PipelineRunner runner) {
      List<Stage.Info> infos = new ArrayList<>(stages.length);
      List<Stage.Info> infosUnmodifiable = Collections.unmodifiableList(infos);
      for (StageRuntime stage : stages) {
        infos.add(stage.getInfo());
        stage.setContext(new StageContext(infosUnmodifiable, runner.getMetrics(), stage));
      }
    }

    private Pipe[] createPipes(StageRuntime[] stages) throws PipelineRuntimeException {
      LaneResolver laneResolver = new LaneResolver(stages);
      List<Pipe> pipes = new ArrayList<>(stages.length * 3);
      for (int idx = 0; idx < stages.length; idx++) {
        Pipe pipe;
        StageRuntime stage = stages[idx];
        switch (stage.getDefinition().getType()) {
          case SOURCE:
            pipe = new StagePipe(stage, laneResolver.getStageInputLanes(idx), laneResolver.getStageOutputLanes(idx));
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
                                 laneResolver.getStageOutputLanes(idx));
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
            pipe = new StagePipe(stage, laneResolver.getStageInputLanes(idx), laneResolver.getStageOutputLanes(idx));
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
