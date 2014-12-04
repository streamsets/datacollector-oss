/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.api.Stage;
import org.junit.Test;
import org.mockito.Mockito;

public class TestObserverPipe {

  @Test
  @SuppressWarnings("unchecked")
  public void testNullObserver() throws Exception {
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Pipeline pipeline = new Pipeline.Builder(MockStages.createStageLibrary(), "name",
                                             MockStages.createPipelineConfigurationSourceTarget()).build(pipelineRunner);
    ObserverPipe pipe = (ObserverPipe) pipeline.getPipes()[1];
    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    pipe.process(pipeBatch);
    Mockito.verify(pipeBatch, Mockito.times(1)).moveLane(Mockito.anyString(), Mockito.anyString());
    Mockito.verifyNoMoreInteractions(pipeBatch);
  }

  @SuppressWarnings("unchecked")
  private void testObserver(boolean observing) throws Exception {
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Observer observer = Mockito.mock(Observer.class);
    Mockito.when(observer.isObserving(Mockito.any(Stage.Info.class))).thenReturn(observing);
    Pipeline pipeline = new Pipeline.Builder(MockStages.createStageLibrary(), "name",
                                             MockStages.createPipelineConfigurationSourceTarget()).setObserver(observer).build(pipelineRunner);
    ObserverPipe pipe = (ObserverPipe) pipeline.getPipes()[1];
    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    pipe.process(pipeBatch);
    Mockito.verify(observer, Mockito.times(1)).isObserving(Mockito.any(Stage.Info.class));
    Mockito.verify(pipeBatch, Mockito.times(1)).moveLane(Mockito.anyString(), Mockito.anyString());
    if (observing) {
      Mockito.verify(observer, Mockito.times(1)).observe(Mockito.eq(pipe), Mockito.anyMap());
      Mockito.verify(pipeBatch, Mockito.times(1)).getPipeLanesSnapshot(Mockito.anyList());
    } else {
      Mockito.verifyNoMoreInteractions(observer);
      Mockito.verifyNoMoreInteractions(pipeBatch);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testObserverOff() throws Exception {
    testObserver(false);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testObserverOn() throws Exception {
    testObserver(true);
  }

}
