/*
 * Copyright 2017 StreamSets Inc.
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
