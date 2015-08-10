/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.FullPipeBatch;
import com.streamsets.datacollector.runner.Observer;
import com.streamsets.datacollector.runner.ObserverPipe;
import com.streamsets.datacollector.runner.PipeBatch;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRunner;

import com.streamsets.datacollector.util.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class TestObserverPipe {

  @Test
  @SuppressWarnings("unchecked")
  public void testNullObserver() throws Exception {
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
    Pipeline pipeline = new Pipeline.Builder(MockStages.createStageLibrary(), new Configuration(), "name", "name", "0",
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
    Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
    Observer observer = Mockito.mock(Observer.class);
    Mockito.when(observer.isObserving(Mockito.any(List.class))).thenReturn(observing);
    Pipeline pipeline = new Pipeline.Builder(MockStages.createStageLibrary(), new Configuration(), "name", "name", "0",
                                             MockStages.createPipelineConfigurationSourceTarget()).setObserver(observer)
                                             .build(pipelineRunner);
    ObserverPipe pipe = (ObserverPipe) pipeline.getPipes()[1];
    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    pipe.process(pipeBatch);
    Mockito.verify(observer, Mockito.times(1)).isObserving(Mockito.any(List.class));
    Mockito.verify(pipeBatch, Mockito.times(1)).moveLane(Mockito.anyString(), Mockito.anyString());
    if (observing) {
      Mockito.verify(observer, Mockito.times(1)).observe(Mockito.eq(pipe), Mockito.anyMap());
      Mockito.verify(pipeBatch, Mockito.times(1)).getLaneOutputRecords(Mockito.anyList());
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
