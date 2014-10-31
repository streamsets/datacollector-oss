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
package com.streamsets.pipeline.runner;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.stagelibrary.StageLibrary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestStageRuntime {

  public static class TSource extends BaseSource {
    public boolean inited;
    public boolean destroyed;
    public String stringVar;
    public int intVar;
    public long longVar;
    public boolean booleanVar;

    @Override
    protected void init() throws StageException {
      inited = true;
    }

    @Override
    public void destroy() {
      destroyed = true;
    }

    @Override
    public String produce(String lastSourceOffset, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

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
    StageDefinition sourceDef = new StageDefinition(TSource.class.getName(), "source", "1.0.0", "label", "description",
                                                   StageType.SOURCE, configDefs);
    sourceDef.setLibrary("library", Thread.currentThread().getContextClassLoader());
    StageDefinition targetDef = new StageDefinition(TTarget.class.getName(), "target", "1.0.0", "label", "description",
                                                    StageType.TARGET, Collections.EMPTY_LIST);
    targetDef.setLibrary("library", Thread.currentThread().getContextClassLoader());
    Mockito.when(lib.getStage(Mockito.eq("library"), Mockito.eq("source"), Mockito.eq("1.0.0"))).thenReturn(sourceDef);
    Mockito.when(lib.getStage(Mockito.eq("library"), Mockito.eq("target"), Mockito.eq("1.0.0"))).thenReturn(targetDef);
    return lib;
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createMockPipelineConfiguration() {
    List<ConfigConfiguration> configs = new ArrayList<ConfigConfiguration>();
    ConfigConfiguration config = new ConfigConfiguration("string", "STRING");
    configs.add(config);
    config = new ConfigConfiguration("int", 1);
    configs.add(config);
    config = new ConfigConfiguration("long", 2);
    configs.add(config);
    config = new ConfigConfiguration("boolean", true);
    configs.add(config);
    List<StageConfiguration> stages = new ArrayList<StageConfiguration>();
    StageConfiguration source = new StageConfiguration("isource", "library", "source", "1.0.0",
                                                      configs, null, Collections.EMPTY_LIST, ImmutableList.of("a"));
    stages.add(source);
    StageConfiguration stage = new StageConfiguration("itarget", "library", "target", "1.0.0",
                                                      Collections.EMPTY_LIST, null, ImmutableList.of("a"),
                                                      Collections.EMPTY_LIST);
    stages.add(stage);
    return new PipelineConfiguration(UUID.randomUUID(), stages, PipelineConfiguration.OnError.DROP_RECORD);
  }

  @Test
  public void testBuilderValidPipeline() throws Exception {
    StageLibrary stageLibrary = createMockStageLibrary();
    PipelineConfiguration pipelineConf = createMockPipelineConfiguration();
    StageRuntime.Builder builder = new StageRuntime.Builder(stageLibrary, pipelineConf);
    StageRuntime[] runtimes = builder.build();
    Assert.assertEquals(2, runtimes.length);
    Assert.assertEquals(stageLibrary.getStage("library", "source", "1.0.0"), runtimes[0].getDefinition());
    Assert.assertEquals(pipelineConf.getStages().get(0), runtimes[0].getConfiguration());
    Assert.assertTrue(runtimes[0].getStage() instanceof TSource);
    Assert.assertEquals("STRING", ((TSource) runtimes[0].getStage()).stringVar);
    Assert.assertEquals(1, ((TSource) runtimes[0].getStage()).intVar);
    Assert.assertEquals(2l, ((TSource) runtimes[0].getStage()).longVar);
    Assert.assertEquals(true, ((TSource) runtimes[0].getStage()).booleanVar);
    Assert.assertEquals("isource", runtimes[0].getInfo().getInstanceName());
    Assert.assertEquals("source", runtimes[0].getInfo().getName());
    Assert.assertEquals("1.0.0", runtimes[0].getInfo().getVersion());
    Assert.assertNotNull(runtimes[1].getDefinition());
    Assert.assertNotNull(runtimes[1].getConfiguration());
    Assert.assertTrue(runtimes[1].getStage() instanceof TTarget);
    Assert.assertEquals(stageLibrary.getStage("library", "target", "1.0.0"), runtimes[1].getDefinition());
    Assert.assertEquals(pipelineConf.getStages().get(1), runtimes[1].getConfiguration());
  }

  @Test(expected = PipelineRuntimeException.class)
  public void testBuilderInvalidPipeline() throws Exception {
    StageLibrary stageLibrary = createMockStageLibrary();
    PipelineConfiguration pipelineConf = createMockPipelineConfiguration();
    pipelineConf.getStages().remove(1);
    StageRuntime.Builder builder = new StageRuntime.Builder(stageLibrary, pipelineConf);
    StageRuntime[] runtimes = builder.build();
  }

  @Test
  public void testBuilderValidPipelineContextInitDestroy() throws Exception {
    StageLibrary stageLibrary = createMockStageLibrary();
    PipelineConfiguration pipelineConf = createMockPipelineConfiguration();
    StageRuntime.Builder builder = new StageRuntime.Builder(stageLibrary, pipelineConf);
    StageRuntime[] runtimes = builder.build();
    Assert.assertFalse(((TSource)runtimes[0].getStage()).inited);
    Source.Context context = Mockito.mock(Source.Context.class);
    runtimes[0].setContext(context);
    Assert.assertEquals(context, runtimes[0].getContext());
    runtimes[0].init();
    Assert.assertTrue(((TSource) runtimes[0].getStage()).inited);
    Assert.assertFalse(((TSource) runtimes[0].getStage()).destroyed);
    runtimes[0].destroy();
    Assert.assertTrue(((TSource) runtimes[0].getStage()).destroyed);
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderValidPipelineContextNotSet() throws Exception {
    StageLibrary stageLibrary = createMockStageLibrary();
    PipelineConfiguration pipelineConf = createMockPipelineConfiguration();
    StageRuntime.Builder builder = new StageRuntime.Builder(stageLibrary, pipelineConf);
    StageRuntime[] runtimes = builder.build();
    Assert.assertFalse(((TSource)runtimes[0].getStage()).inited);
    runtimes[0].init();
  }

}
