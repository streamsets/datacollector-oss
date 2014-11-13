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

import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.runner.Pipe;
import com.streamsets.pipeline.runner.PipelineRunner;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.StageRuntime;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestPreviewPipelineRunner {

  @Test
  public void testRunner() throws Exception {
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PipelineRunner runner = new PreviewPipelineRunner(tracker, -1, 1, true);
    Assert.assertNotNull(runner.getMetrics());
    Assert.assertTrue(runner.getBatchesOutput().isEmpty());

    StageDefinition def = Mockito.mock(StageDefinition.class);
    Mockito.when(def.getType()).thenReturn(StageType.SOURCE);
    StageRuntime stage = Mockito.mock(StageRuntime.class);
    Mockito.when(stage.getDefinition()).thenReturn(def);
    Pipe pipe = Mockito.mock(Pipe.class);
    Mockito.when(pipe.getStage()).thenReturn(stage);
    Pipe[] pipes = { pipe };
    runner.run(pipes);
    Assert.assertNotNull(runner.getBatchesOutput());

  }


}
