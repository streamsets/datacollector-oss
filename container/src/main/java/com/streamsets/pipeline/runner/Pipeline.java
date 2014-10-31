/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.runner;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.container.Configuration;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.stagelibrary.StageLibrary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class Pipeline {
  private final Pipe[] pipes;
  private final PipelineRunner runner;

  private Pipeline(Pipe[] pipes, PipelineRunner runner) {
    this.pipes = pipes;
    this.runner = runner;
  }

  public void reconfigure(Configuration conf) {
    for (Pipe pipe : pipes) {
      try {
        pipe.reconfigure(conf);
      } catch (RuntimeException ex) {
        //LOG WARN
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void init() throws StageException {
    int idx = 0;
    try {
      for (; idx < pipes.length; idx++) {
        pipes[idx].init();
      }
    } catch (StageException ex) {
      destroy(idx);
      throw ex;
    } catch (RuntimeException ex) {
      destroy(idx);
      throw new RuntimeException(ex);
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
    runner.run(pipes);
  }

  static class StageContext implements Source.Context, Target.Context, Processor.Context {
    private final List<Stage.Info> pipelineInfo;
    private final MetricRegistry metrics;
    private final String instanceName;
    private final Set<String> outputLanes;

    public StageContext(List<Stage.Info> pipelineInfo, MetricRegistry metrics, StageConfiguration conf) {
      this.pipelineInfo = pipelineInfo;
      this.metrics = metrics;
      this.instanceName = conf.getInstanceName();
      this.outputLanes = ImmutableSet.copyOf(conf.getOutputLanes());

    }

    @Override
    public List<Stage.Info> getPipelineInfo() {
      return pipelineInfo;
    }

    @Override
    public MetricRegistry getMetrics() {
      return metrics;
    }

    @Override
    public Set<String> getOutputLanes() {
      return outputLanes;
    }

    @Override
    public Record createRecord(String sourceInfo) {
      return new RecordImpl(instanceName, sourceInfo, null, null);
    }

    @Override
    public Record createRecord(String sourceInfo, byte[] raw, String rawMime) {
      return new RecordImpl(instanceName, sourceInfo, raw, rawMime);
    }

    @Override
    public Record cloneRecord(Record record) {
      return new RecordImpl(instanceName, (RecordImpl)record);
    }
  }

  public static class Builder {
    private final StageLibrary stageLib;
    private final PipelineConfiguration pipelineConf;

    public Builder(StageLibrary stageLib, PipelineConfiguration pipelineConf) {
      this.stageLib = stageLib;
      this.pipelineConf = pipelineConf;
    }

    public Pipeline build(PipelineRunner runner) throws PipelineRuntimeException {
      StageRuntime[] stages = new StageRuntime.Builder(stageLib, pipelineConf).build();
      setStagesContext(stages, runner);
      Pipe[] pipes = createPipes(stages);
      return new Pipeline(pipes, runner);
    }

    void setStagesContext(StageRuntime[] stages, PipelineRunner runner) {
      List<Stage.Info> infos = new ArrayList<Stage.Info>(stages.length);
      List<Stage.Info> infosUnmodifiable = Collections.unmodifiableList(infos);
      for (StageRuntime stage : stages) {
        infos.add(stage.getInfo());
        stage.setContext(new StageContext(infosUnmodifiable, runner.getMetrics(), stage.getConfiguration()));
      }
    }

    @VisibleForTesting
    protected List<String> computeMultiplexerOutputLanes(StageRuntime currentStage, StageRuntime[] stages, int stageIdx) {
      List<String> outputLanes = new ArrayList<String>();
      for (String output : currentStage.getConfiguration().getOutputLanes()) {
        for (int i = stageIdx + 1; i < stages.length; i++) {
          for (String input : stages[i].getConfiguration().getInputLanes()) {
            if (input.equals(output)) {
              outputLanes.add(output + "::" + input);
            }
          }
        }
      }
      return Collections.unmodifiableList(outputLanes);
    }

    @VisibleForTesting
    protected List<String> computeCombinerInputLanes(StageRuntime currentStage, StageRuntime[] stages, int stageIdx) {
      List<String> inputLanes = new ArrayList<String>();
      for (String input : currentStage.getConfiguration().getInputLanes()) {
        for (int i = 0; i < stageIdx; i++) {
          for (String output : stages[i].getConfiguration().getOutputLanes()) {
            if (output.equals(input)) {
              inputLanes.add(output + "::" + input);
            }
          }
        }
      }
      return Collections.unmodifiableList(inputLanes);
    }

    protected String computeCombinerInput(StageRuntime currentStage) {
      return "::" + currentStage.getConfiguration().getInstanceName();
    }

    Pipe[] createPipes(StageRuntime[] stages) throws PipelineRuntimeException {
      List<Pipe> pipes = new ArrayList<Pipe>(stages.length * 3);
      for (int stageIdx = 0; stageIdx < stages.length; stageIdx++) {
        Pipe pipe;
        StageRuntime stage = stages[stageIdx];
        switch (stage.getDefinition().getType()) {
          case SOURCE:
            pipe = new StagePipe(stage);
            pipes.add(pipe);
            pipe = new ObserverPipe(stage, pipe.getOutputLanes());
            pipes.add(pipe);
            pipe = new MultiplexerPipe(stage, computeMultiplexerOutputLanes(stage, stages, stageIdx));
            pipes.add(pipe);
            break;
          case PROCESSOR:
            pipe = new CombinerPipe(stage, computeCombinerInputLanes(stage, stages, stageIdx),
                                    computeCombinerInput(stage));
            pipes.add(pipe);
            pipe = new StagePipe(stage, computeCombinerInput(stage));
            pipes.add(pipe);
            pipe = new ObserverPipe(stage, pipe.getOutputLanes());
            pipes.add(pipe);
            pipe = new MultiplexerPipe(stage, computeMultiplexerOutputLanes(stage, stages, stageIdx));
            pipes.add(pipe);
            break;
          case TARGET:
            pipe = new CombinerPipe(stage, computeCombinerInputLanes(stage, stages, stageIdx),
                                    computeCombinerInput(stage));
            pipes.add(pipe);
            pipe = new StagePipe(stage, computeCombinerInput(stage));
            pipes.add(pipe);
            break;
          default:
            throw new RuntimeException(String.format("Stage '%s' does not have a stage type",
                                                     stage.getInfo().getInstanceName()));
        }
      }
      return pipes.toArray(new Pipe[pipes.size()]);
    }

  }

}
