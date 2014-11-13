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

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.PipelineDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.stagelibrary.StageLibrary;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.inject.Singleton;
import javax.ws.rs.core.Response;
import java.util.*;

public class TestStageLibraryResource {

  private StageLibraryResource r = null;

  /**
   * Mock source implementation
   */
  public static class TSource extends BaseSource {
    public boolean inited;
    public boolean destroyed;

    @Override
    protected void init() throws StageException {
      inited = true;
    }

    @Override
    public void destroy() {
      destroyed = true;
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  /**
   * Mock target implementation
   */
  public static class TTarget extends BaseTarget {
    public boolean inited;
    public boolean destroyed;

    @Override
    protected void init() throws StageException {
      inited = true;
    }

    @Override
    public void destroy() {
      destroyed = true;
    }
    @Override
    public void write(Batch batch) throws StageException {
    }
  }

  @SuppressWarnings("unchecked")
  /**
   *
   * @return Mock stage library implementation
   */
  public static StageLibrary createMockStageLibrary() {
    StageLibrary lib = Mockito.mock(StageLibrary.class);
    List<ConfigDefinition> configDefs = new ArrayList<ConfigDefinition>();
    ConfigDefinition configDef = new ConfigDefinition("string", ConfigDef.Type.STRING, "l1", "d1", "--", true, "g",
      "stringVar", null);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("int", ConfigDef.Type.INTEGER, "l2", "d2", "-1", true, "g", "intVar", null);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("long", ConfigDef.Type.INTEGER, "l3", "d3", "-2", true, "g", "longVar", null);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("boolean", ConfigDef.Type.BOOLEAN, "l4", "d4", "false", true, "g", "booleanVar", null);
    configDefs.add(configDef);
    StageDefinition sourceDef = new StageDefinition(
      TSource.class.getName(), "source", "1.0.0", "label", "description",
      StageType.SOURCE, configDefs, StageDef.OnError.DROP_RECORD, "");
    sourceDef.setLibrary("library", Thread.currentThread().getContextClassLoader());
    StageDefinition targetDef = new StageDefinition(
      TTarget.class.getName(), "target", "1.0.0", "label", "description",
      StageType.TARGET, Collections.EMPTY_LIST, StageDef.OnError.DROP_RECORD, "TargetIcon.svg");
    targetDef.setLibrary("library", Thread.currentThread().getContextClassLoader());
    Mockito.when(lib.getStage(Mockito.eq("library"), Mockito.eq("source"), Mockito.eq("1.0.0"))).thenReturn(sourceDef);
    Mockito.when(lib.getStage(Mockito.eq("library"), Mockito.eq("target"), Mockito.eq("1.0.0"))).thenReturn(targetDef);

    List<StageDefinition> stages = new ArrayList<StageDefinition>(2);
    stages.add(sourceDef);
    stages.add(targetDef);
    Mockito.when(lib.getStages(Mockito.any(Locale.class))).thenReturn(stages);
    return lib;
  }

  @Module(injects = StageLibraryResource.class)
  public static class TestStageLibraryResourceModule {

    public TestStageLibraryResourceModule() {
    }

    @Provides
    @Singleton
    public StageLibrary provideStageLibrary() {
      return createMockStageLibrary();
    }
  }

  @Before
  public void setUp() {
    ObjectGraph dagger = ObjectGraph.create(TestStageLibraryResourceModule.class);
    r = dagger.get(StageLibraryResource.class);
  }

  @Test
  public void testGetAllModules() {
    Response response = r.getDefinitions();
    Map<String, List<Object>> definitions = (Map<String, List<Object>>)response.getEntity();

    //check the pipeline definition
    Assert.assertTrue(definitions.containsKey("pipeline"));
    List<Object> pipelineDefinition = definitions.get("pipeline");
    Assert.assertTrue(pipelineDefinition.size() == 1);
    Assert.assertTrue(pipelineDefinition.get(0) instanceof PipelineDefinition);

    PipelineDefinition pd = (PipelineDefinition)pipelineDefinition.get(0);
    Assert.assertNotNull(pd.getConfigDefinitions());
    Assert.assertEquals(2, pd.getConfigDefinitions().size());

    //check the stages
    Assert.assertTrue(definitions.containsKey("stages"));
    List<Object> stages = definitions.get("stages");
    Assert.assertEquals(2, stages.size());
    //check the first stage
    Assert.assertTrue(stages.get(0) instanceof StageDefinition);
    StageDefinition s1 = (StageDefinition) stages.get(0);
    Assert.assertNotNull(s1);
    Assert.assertEquals("source", s1.getName());
    Assert.assertEquals("com.streamsets.pipeline.restapi.TestStageLibraryResource$TSource", s1.getClassName());
    Assert.assertEquals("1.0.0", s1.getVersion());
    Assert.assertEquals("label", s1.getLabel());
    Assert.assertEquals("description", s1.getDescription());
    Assert.assertEquals(4, s1.getConfigDefinitions().size());

    Assert.assertTrue(stages.get(1) instanceof StageDefinition);
    StageDefinition s2 = (StageDefinition) stages.get(1);
    Assert.assertNotNull(s2);
    Assert.assertEquals("target", s2.getName());
    Assert.assertEquals("com.streamsets.pipeline.restapi.TestStageLibraryResource$TTarget", s2.getClassName());
    Assert.assertEquals("1.0.0", s2.getVersion());
    Assert.assertEquals("label", s2.getLabel());
    Assert.assertEquals("description", s2.getDescription());
    Assert.assertTrue(s2.getConfigDefinitions().isEmpty());
  }

  @Test
  public void testGetIcon() {
    Response response = r.getIcon("target", "library", "1.0.0");
    response.getEntity();
  }

}