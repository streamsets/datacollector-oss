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
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.state.*;
import com.streamsets.pipeline.store.PipelineStoreTask;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.inject.Singleton;
import javax.ws.rs.core.Response;
import java.util.Arrays;

import static org.mockito.Matchers.any;

public class TestPipelineManagerResource {

  private PipelineManagerResource r;

  @Module(injects = PipelineManagerResource.class)
  public static class TestPipelineManagerResourceModule {

    public TestPipelineManagerResourceModule() {
    }

    @Provides
    @Singleton
    public StageLibraryTask provideStageLibrary() {
      return Mockito.mock(StageLibraryTask.class);
    }

    @Provides
    @Singleton
    public Configuration provideConfiguration() {
      return Mockito.mock(Configuration.class);
    }

    @Provides
    @Singleton
    public PipelineStoreTask providePipelineStore() {
      return Mockito.mock(PipelineStoreTask.class);
    }

    @Provides
    public PipelineManagerTask providePipelineManager() {
      System.setProperty("pipeline.data.dir", "./target/var");
      RuntimeInfo info = new RuntimeInfo(Arrays.asList(getClass().getClassLoader()));
      PipelineManagerTask manager = new PipelineManagerTask(info);
      manager.init();

      return manager;
    }
  }

  @Before
  public void setUp() {
    ObjectGraph dagger = ObjectGraph.create(TestPipelineManagerResourceModule.class);
    r = dagger.get(PipelineManagerResource.class);
  }

  @Test
  public void testGetStatus() throws PipelineStateException {
    Response response = r.getStatus();
    PipelineState pipelineState = (PipelineState)response.getEntity();

    Assert.assertNotNull(pipelineState);
    Assert.assertEquals(State.NOT_RUNNING, pipelineState.getPipelineState());

  }

}
