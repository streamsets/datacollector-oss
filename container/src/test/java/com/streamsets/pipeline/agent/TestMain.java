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
package com.streamsets.pipeline.agent;

import dagger.Module;
import dagger.Provides;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class TestMain {
  private static Runtime runtime = Mockito.mock(Runtime.class);
  private static LogConfigurator logConfigurator = Mockito.mock(LogConfigurator.class);
  private static BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
  private static RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
  private static Task task = Mockito.mock(Task.class);

  @Module(injects = {TaskWrapper.class, LogConfigurator.class, BuildInfo.class, RuntimeInfo.class})
  public static class TPipelineAgentModule {

    @Provides
    public LogConfigurator provideLogConfigurator() {
      return logConfigurator;
    }

    @Provides 
    public BuildInfo provideBuildInfo() {
      return buildInfo;
    }

    @Provides
    public RuntimeInfo provideRuntimeInfo() {
      return runtimeInfo;
    }

    @Provides
    public Task provideAgent() {
      return task;
    }
  }

  public static class TMain extends Main {

    public TMain() {
      super(TPipelineAgentModule.class);
    }

    @Override
    Runtime getRuntime() {
      return runtime;
    }
  }

  @Before
  public void before() {
    Mockito.reset(runtime);
    Mockito.reset(logConfigurator);
    Mockito.reset(buildInfo);
    Mockito.reset(runtimeInfo);
    Mockito.reset(task);
  }

  @Test
  public void testMainClassGetRuntime() {
    Main main = new Main();
    Assert.assertEquals(Runtime.getRuntime(), main.getRuntime());
  }

  @Test
  public void testOKFullRun() {
    Main main = new TMain();
    Mockito.verifyZeroInteractions(runtime);
    Mockito.verifyZeroInteractions(logConfigurator);
    Mockito.verifyZeroInteractions(buildInfo);
    Mockito.verifyZeroInteractions(runtimeInfo);
    Mockito.verifyZeroInteractions(task);
    Assert.assertEquals(0, main.doMain());
    Mockito.verify(logConfigurator, Mockito.times(1)).configure();
    Mockito.verify(buildInfo, Mockito.times(1)).log(Mockito.any(Logger.class));
    Mockito.verify(runtimeInfo, Mockito.times(1)).log(Mockito.any(Logger.class));
    Mockito.verify(task, Mockito.times(1)).init();
    Mockito.verify(runtime, Mockito.times(1)).addShutdownHook(Mockito.any(Thread.class));
    Mockito.verify(task, Mockito.times(1)).run();
    Mockito.verify(runtime, Mockito.times(1)).removeShutdownHook(Mockito.any(Thread.class));
  }

  @Test
  public void testInitException() {
    Mockito.doThrow(new RuntimeException()).when(task).init();
    Main main = new TMain();
    Assert.assertEquals(1, main.doMain());
    Mockito.verify(logConfigurator, Mockito.times(1)).configure();
    Mockito.verify(buildInfo, Mockito.times(1)).log(Mockito.any(Logger.class));
    Mockito.verify(runtimeInfo, Mockito.times(1)).log(Mockito.any(Logger.class));
    Mockito.verify(task, Mockito.times(1)).init();
    Mockito.verify(runtime, Mockito.times(0)).addShutdownHook(Mockito.any(Thread.class));
    Mockito.verify(task, Mockito.times(0)).run();
    Mockito.verify(runtime, Mockito.times(0)).removeShutdownHook(Mockito.any(Thread.class));
  }

  @Test
  public void testRunException() {
    Mockito.doThrow(new RuntimeException()).when(task).run();
    Main main = new TMain();
    Assert.assertEquals(1, main.doMain());
    Mockito.verify(logConfigurator, Mockito.times(1)).configure();
    Mockito.verify(buildInfo, Mockito.times(1)).log(Mockito.any(Logger.class));
    Mockito.verify(runtimeInfo, Mockito.times(1)).log(Mockito.any(Logger.class));
    Mockito.verify(task, Mockito.times(1)).init();
    Mockito.verify(runtime, Mockito.times(1)).addShutdownHook(Mockito.any(Thread.class));
    Mockito.verify(task, Mockito.times(1)).run();
    Mockito.verify(runtime, Mockito.times(0)).removeShutdownHook(Mockito.any(Thread.class));
  }

  @Test
  public void testShutdownHook() {
    ArgumentCaptor<Thread> shutdownHookCaptor = ArgumentCaptor.forClass(Thread.class);
    Main main = new TMain();
    Assert.assertEquals(0, main.doMain());
    Mockito.verify(runtime, Mockito.times(1)).addShutdownHook(shutdownHookCaptor.capture());
    Mockito.reset(task);
    shutdownHookCaptor.getValue().run();
    Mockito.verify(task, Mockito.times(1)).stop();
  }

}
