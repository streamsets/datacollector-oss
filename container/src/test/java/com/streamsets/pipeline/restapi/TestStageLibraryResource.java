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
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.inject.Singleton;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;

public class TestStageLibraryResource extends JerseyTest {

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
  public static StageLibraryTask createMockStageLibrary() {
    StageLibraryTask lib = Mockito.mock(StageLibraryTask.class);
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
    Mockito.when(lib.getStages()).thenReturn(stages);
    return lib;
  }

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(new StageLibraryResourceConfig());
        register(StageLibraryResource.class);
      }
    };
  }
  static class StageLibraryResourceConfig extends AbstractBinder {
    @Override
    protected void configure() {
      bindFactory(StageLibraryTestInjector.class).to(StageLibraryTask.class);
    }
  }

  static class StageLibraryTestInjector implements Factory<StageLibraryTask> {

    public StageLibraryTestInjector() {
    }

    @Singleton
    @Override
    public StageLibraryTask provide() {
      return createMockStageLibrary();
    }

    @Override
    public void dispose(StageLibraryTask stageLibrary) {
    }
  }


  @Test
  public void testGetAllModules() {

    Response response = target("/v1/definitions").request().get();
    Map<String, List<Object>> definitions = response.readEntity(new GenericType<Map<String, List<Object>>>() {});

    //check the pipeline definition
    Assert.assertTrue(definitions.containsKey("pipeline"));
    List<Object> pipelineDefinition = definitions.get("pipeline");
    Assert.assertTrue(pipelineDefinition.size() == 1);

    //check the stages
    Assert.assertTrue(definitions.containsKey("stages"));
    List<Object> stages = definitions.get("stages");
    Assert.assertEquals(2, stages.size());

    //TODO The json is deserialized as a generic map
    /*Assert.assertTrue(pipelineDefinition.get(0) instanceof PipelineDefinition);

    PipelineDefinition pd = (PipelineDefinition)pipelineDefinition.get(0);
    Assert.assertNotNull(pd.getConfigDefinitions());
    Assert.assertEquals(2, pd.getConfigDefinitions().size());*/

    /*//check the first stage
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
    Assert.assertTrue(s2.getConfigDefinitions().isEmpty());*/
  }

  @Test
  public void testGetIcon() throws IOException {
    Response response = target("/v1/definitions/stages/icon").queryParam("name", "target")
        .queryParam("library", "library").queryParam("version", "1.0.0").request().get();
    Assert.assertTrue(response.getEntity() != null);
  }

}