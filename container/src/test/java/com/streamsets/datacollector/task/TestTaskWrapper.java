/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.task;

import org.junit.Test;
import org.mockito.Mockito;

import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.task.TaskWrapper;

public class TestTaskWrapper {

  @Test
  public void testMainAgentDelegation() throws Exception {
    Task mock = Mockito.mock(Task.class);
    TaskWrapper agent = new TaskWrapper(mock);
    agent.init();
    Mockito.verify(mock, Mockito.times(1)).init();
    Mockito.verifyNoMoreInteractions(mock);
    agent.run();
    Mockito.verify(mock, Mockito.times(1)).run();
    Mockito.verifyNoMoreInteractions(mock);
    agent.stop();
    Mockito.verify(mock, Mockito.times(1)).stop();
    Mockito.verifyNoMoreInteractions(mock);
    agent.waitWhileRunning();
    Mockito.verify(mock, Mockito.times(1)).waitWhileRunning();
    Mockito.verifyNoMoreInteractions(mock);
    agent.getName();
    Mockito.verify(mock, Mockito.times(1)).getName();
    Mockito.verifyNoMoreInteractions(mock);
    agent.getStatus();
    Mockito.verify(mock, Mockito.times(1)).getStatus();
    Mockito.verifyNoMoreInteractions(mock);
  }

}
