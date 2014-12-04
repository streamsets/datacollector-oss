/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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

  //@Test
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
