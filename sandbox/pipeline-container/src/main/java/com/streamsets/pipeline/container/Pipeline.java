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
package com.streamsets.pipeline.container;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Module;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.Configuration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Pipeline {

  public static class Builder {
    private boolean built;
    private List<Pipe> pipes;
    private Configuration conf;

    public Builder(Module.Info info, Source source, Set<String> output) {
      this(info, source, output, null);
    }

    public Builder(Module.Info info, Source source, Set<String> output, Observer observer) {
      Preconditions.checkNotNull(info, "info cannot be null");
      Preconditions.checkNotNull(source, "source cannot be null");
      Preconditions.checkNotNull(output, "output cannot be null");
      Preconditions.checkArgument(!output.isEmpty(), "output cannot be empty");
      pipes = new ArrayList<Pipe>();
      SourcePipe sourcePipe = new SourcePipe(info, source, output);
      pipes.add(sourcePipe);
      if (observer != null) {
        ObserverPipe observerPipe = new ObserverPipe(sourcePipe, observer);
        pipes.add(observerPipe);
      }
    }

    public Builder add(Module.Info info, Processor processor, Set<String> input, Set<String> output) {
      return add(info, processor, input, output, null);
    }

    public Builder add(Module.Info info, Processor processor, Set<String> input, Set<String> output,
        Observer observer) {
      Preconditions.checkNotNull(info, "info cannot be null");
      Preconditions.checkNotNull(processor, "processor cannot be null");
      Preconditions.checkNotNull(input, "input cannot be null");
      Preconditions.checkNotNull(output, "output cannot be null");
      Preconditions.checkArgument(!input.isEmpty(), "input cannot be empty");
      Preconditions.checkArgument(!output.isEmpty(), "output cannot be empty");
      ProcessorPipe processorPipe = new ProcessorPipe(info, processor, input, output);
      pipes.add(processorPipe);
      if (observer != null) {
        ObserverPipe observerPipe = new ObserverPipe(processorPipe, observer);
        pipes.add(observerPipe);
      }
      return this;
    }

    public Builder add(Module.Info info, Target target, Set<String> input) {
      Preconditions.checkNotNull(info, "info cannot be null");
      Preconditions.checkNotNull(target, "target cannot be null");
      Preconditions.checkNotNull(input, "input cannot be null");
      Preconditions.checkArgument(!input.isEmpty(), "input cannot be empty");
      pipes.add(new TargetPipe(info, target, input));
      return this;
    }

    public void setConfiguration(Configuration conf) {
      Preconditions.checkNotNull(conf, "conf cannot be null");
      this.conf = conf;
    }

    public Builder validate() {
      Pipeline.validate(pipes.toArray(new Pipe[pipes.size()]));
      return this;
    }

    public Pipeline build() {
      Preconditions.checkState(!built, "Builder has been built already, it cannot be reused");
      Pipeline pipeline = new Pipeline(pipes.toArray(new Pipe[pipes.size()]));
      if (conf != null) {
        pipeline.configure(conf);
      }
      built = true;
      return pipeline;
    }

  }

  private Pipe[] pipes;

  private Pipeline(Pipe[] pipes) {
    validate(pipes);
    this.pipes = pipes;
  }

  private static void validate(Pipe[] pipes) {
    Preconditions.checkNotNull(pipes, "pipes cannot be null");
    Set<String> moduleNames = new HashSet<String>();
    Set<String> currentLines = new HashSet<String>();
    for (Pipe pipe : pipes) {
      Preconditions.checkState(!moduleNames.contains(pipe.getName()), String.format(
          "Pipe '%s' already exists", pipe.getName()));
      moduleNames.add(pipe.getName());
      Preconditions.checkState(currentLines.containsAll(pipe.getInputLanes()), String.format(
          "Pipe '%s' requires a input line which is not available", pipe.getName()));
      currentLines.removeAll(pipe.getConsumedLanes());
      currentLines.addAll(pipe.getOutputLanes());
    }
    Preconditions.checkState(currentLines.isEmpty(), String.format(
        "End of pipeline should not have any line, it has: %s", currentLines));
  }

  public void configure(Configuration conf) {
    Preconditions.checkNotNull(conf, "conf cannot be null");

    // configure pipeline
    Configuration pipelineConf = conf.getSubSetConfiguration("pipeline.");
    // TODO

    // configuring pipes
    for (Pipe pipe : pipes) {
      pipe.configure(conf.getSubSetConfiguration(pipe.getName()));
    }
  }

  public void runBatch(PipelineBatch batch) {
    Preconditions.checkNotNull(batch, "batch cannot be null");
    for (Pipe pipe : pipes) {
      batch.createLines(pipe.getOutputLanes());
      pipe.processBatch(batch);
      batch.deleteLines(pipe.getConsumedLanes());
    }
    Preconditions.checkState(batch.isEmpty(), String.format("Batch should be empty, it has: %s", batch.getLanes()));
  }

}
