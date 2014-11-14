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
package com.streamsets.pipeline.task;

import com.streamsets.pipeline.task.Task;
import com.streamsets.pipeline.task.TaskWrapper;
import org.junit.Test;
import org.mockito.Mockito;

public class TestTaskWrapper {

  @Test
  public void testMainAgentDelegation() {
    Task mock = Mockito.mock(Task.class);
    TaskWrapper agent = new TaskWrapper();
    agent.task = mock;
    agent.init();
    Mockito.verify(mock, Mockito.times(1)).init();
    Mockito.verifyNoMoreInteractions(mock);
    agent.run();
    Mockito.verify(mock, Mockito.times(1)).run();
    Mockito.verifyNoMoreInteractions(mock);
    agent.stop();
    Mockito.verify(mock, Mockito.times(1)).stop();
    Mockito.verifyNoMoreInteractions(mock);
  }

}
