/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.creation;

import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.config.StageLibraryDelegateDefinitition;
import com.streamsets.datacollector.definition.StageLibraryDelegateDefinitionExtractor;
import com.streamsets.datacollector.runner.StageLibraryDelegateRuntime;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.delegate.BaseStageLibraryDelegate;
import com.streamsets.pipeline.api.delegate.StageLibraryDelegateDef;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestStageLibraryDelegateCreator {

  @StageLibraryDelegateDef(Runnable.class)
  public static class RunnableDelegate extends BaseStageLibraryDelegate implements Runnable {

    boolean called = false;

    @Override
    public void run() {
      this.called = true;
    }
  }

  private StageLibraryDefinition stageLibrary;
  private StageLibraryDelegateDefinitition delegateDefinitition;
  private StageLibraryTask stageLibraryTask;

  @Before
  public void setUp() {
    this.stageLibrary = Mockito.mock(StageLibraryDefinition.class);
    this.delegateDefinitition = StageLibraryDelegateDefinitionExtractor.get().extract(stageLibrary, RunnableDelegate.class);
    this.stageLibraryTask = Mockito.mock(StageLibraryTask.class);
    Mockito.when(stageLibraryTask.getStageLibraryDelegateDefinition("secret", Runnable.class)).thenReturn(delegateDefinitition);
  }

  @Test
  public void testCreation() {
    Runnable instance = StageLibraryDelegateCreator.get().createAndInitialize(
      stageLibraryTask,
      Mockito.mock(Configuration.class),
      "secret",
      Runnable.class
    );

    // Verify proper wrapped instance
    assertNotNull(instance);
    assertTrue(instance instanceof StageLibraryDelegateRuntime);

    // Run the delegate
    instance.run();

    // Verify that the execution was passed down
    RunnableDelegate delegate = (RunnableDelegate) ((StageLibraryDelegateRuntime)instance).getWrapped();
    assertNotNull(delegate);
    assertTrue(delegate.called);
  }
}
