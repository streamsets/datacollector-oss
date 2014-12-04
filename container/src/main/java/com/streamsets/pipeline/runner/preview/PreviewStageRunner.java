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
package com.streamsets.pipeline.runner.preview;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.runner.Pipe;
import com.streamsets.pipeline.runner.PipeBatch;
import com.streamsets.pipeline.runner.PipelineRunner;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.runner.StagePipe;
import com.streamsets.pipeline.util.ContainerError;

import java.util.ArrayList;
import java.util.List;

public class PreviewStageRunner implements PipelineRunner {
  private final String instanceName;
  private List<Record> inputRecords;
  private final MetricRegistry metrics;
  private final List<List<StageOutput>> batchesOutput;

  public PreviewStageRunner(String instanceName, List<Record> inputRecords) {
    this.instanceName = instanceName;
    this.inputRecords = inputRecords;
    this.metrics = new MetricRegistry();
    this.batchesOutput = new ArrayList<>();
  }

  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  public void run(Pipe[] pipes) throws StageException, PipelineRuntimeException {
    Pipe stagePipe =  null;
    for (Pipe pipe : pipes) {
      if (pipe.getStage().getInfo().getInstanceName().equals(instanceName) && pipe instanceof StagePipe) {
        stagePipe = pipe;
        break;
      }
    }
    if (stagePipe != null) {
      if (stagePipe.getStage().getDefinition().getType() == StageType.SOURCE) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0157, instanceName);
      }
      PipeBatch pipeBatch = new StagePreviewPipeBatch(instanceName, inputRecords);
      stagePipe.process(pipeBatch);
      batchesOutput.add(pipeBatch.getSnapshotsOfAllStagesOutput());

    } else {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0156, instanceName);
    }
  }

  @Override
  public String getSourceOffset() {
    return null;
  }

  @Override
  public String getNewSourceOffset() {
    return null;
  }

  @Override
  public List<List<StageOutput>> getBatchesOutput() {
    return batchesOutput;
  }

}
