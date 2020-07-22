/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.startPipeline;

import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineErrors;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestStartPipelineProcessor {

  @Test
  public void testEmptyPipelineId() throws StageException {
    Processor startPipelineProcessor = new TestStartPipelineProcessorBuilder()
        .taskName("task1")
        .baseUrl("http://invalidHost:19630")
        .pipelineIdConfig("", "{}")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(StartPipelineDProcessor.class, startPipelineProcessor)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    // Configuration value is required for pipeline ID
    Assert.assertTrue(issues.get(0).toString().contains(StartPipelineErrors.START_PIPELINE_03.name()));
  }

  @Test
  public void testInvalidUrl() throws StageException {
    Processor startPipelineProcessor = new TestStartPipelineProcessorBuilder()
        .taskName("task1")
        .baseUrl("http://invalidHost:19630")
        .pipelineIdConfig("samplePipelineId", "{}")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(StartPipelineDProcessor.class, startPipelineProcessor)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    // Failed to connect to execution engine
    Assert.assertTrue(issues.get(0).toString().contains(StartPipelineErrors.START_PIPELINE_01.name()));
  }


}
