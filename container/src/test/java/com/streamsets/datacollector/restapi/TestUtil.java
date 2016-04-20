/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;

import org.glassfish.hk2.api.Factory;
import org.mockito.Mockito;

import javax.inject.Singleton;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class TestUtil {

  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "2.0";
  private static final String DEFAULT_PIPELINE_REV = "0";
  private static final String SNAPSHOT_NAME = "snapshot";

  /**
   * Mock source implementation
   */
  public static class TSource extends BaseSource {
    public boolean inited;
    public boolean destroyed;

    @Override
    protected List<ConfigIssue> init() {
      List<ConfigIssue> issues = super.init();
      inited = true;
      return issues;
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
    protected List<ConfigIssue> init() {
      List<ConfigIssue> issues = super.init();
      inited = true;
      return issues;
    }

    @Override
    public void destroy() {
      destroyed = true;
    }
    @Override
    public void write(Batch batch) throws StageException {
    }
  }

  private static final StageLibraryDefinition MOCK_LIB_DEF =
      new StageLibraryDefinition(TestUtil.class.getClassLoader(), "mock", "MOCK", new Properties(), null, null, null);

  @SuppressWarnings("unchecked")
  /**
   *
   * @return Mock stage library implementation
   */
  public static StageLibraryTask createMockStageLibrary() {
    StageLibraryTask lib = Mockito.mock(StageLibraryTask.class);
    List<ConfigDefinition> configDefs = new ArrayList<>();
    ConfigDefinition configDef = new ConfigDefinition("string", ConfigDef.Type.STRING, "l1", "d1", "--", true, "g",
        "stringVar", null, "", new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
      ConfigDef.Evaluation.IMPLICIT, null);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("int", ConfigDef.Type.NUMBER, "l2", "d2", "-1", true, "g", "intVar", null, "",
      new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
      ConfigDef.Evaluation.IMPLICIT, null);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("long", ConfigDef.Type.NUMBER, "l3", "d3", "-2", true, "g", "longVar", null, "",
      new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
      ConfigDef.Evaluation.IMPLICIT, null);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("boolean", ConfigDef.Type.BOOLEAN, "l4", "d4", "false", true, "g", "booleanVar",
      null, "", new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<Class> emptyList(),
      ConfigDef.Evaluation.IMPLICIT, null);
    configDefs.add(configDef);
    StageDefinition sourceDef = new StageDefinition(
        MOCK_LIB_DEF, false, TSource.class, "source", 1, "label", "description",
        StageType.SOURCE, false, true, true, configDefs, null/*raw source definition*/, "", null, false ,1,
        null, Arrays.asList(ExecutionMode.CLUSTER_BATCH, ExecutionMode.STANDALONE), false, new StageUpgrader.Default(),
        Collections.<String>emptyList(), false, "", false, false);
    StageDefinition targetDef = new StageDefinition(
        MOCK_LIB_DEF, false, TTarget.class, "target", 1, "label", "description",
        StageType.TARGET, false, true, true, Collections.<ConfigDefinition>emptyList(), null/*raw source definition*/,
        "TargetIcon.svg", null, false, 0, null, Arrays.asList(ExecutionMode.CLUSTER_BATCH,
                                                              ExecutionMode.STANDALONE), false,
        new StageUpgrader.Default(), Collections.<String>emptyList(), false, "", false, false);
    Mockito.when(lib.getStage(Mockito.eq("library"), Mockito.eq("source"), Mockito.eq(false)))
           .thenReturn(sourceDef);
    Mockito.when(lib.getStage(Mockito.eq("library"), Mockito.eq("target"), Mockito.eq(false)))
           .thenReturn(targetDef);

    List<StageDefinition> stages = new ArrayList<>(2);
    stages.add(sourceDef);
    stages.add(targetDef);
    Mockito.when(lib.getStages()).thenReturn(stages);

    Mockito.when(lib.getPipeline()).thenReturn(PipelineDefinition.getPipelineDef());
    return lib;
  }

  public static class StageLibraryTestInjector implements Factory<StageLibraryTask> {

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

  public static class URITestInjector implements Factory<URI> {
    @Override
    public URI provide() {
      try {
        return new URI("URIInjector");
      } catch (URISyntaxException e) {
        e.printStackTrace();
        return null;
      }
    }

    @Override
    public void dispose(URI uri) {
    }

  }

  public static class PrincipalTestInjector implements Factory<Principal> {

    @Override
    public Principal provide() {
      return new Principal() {
        @Override
        public String getName() {
          return "nobody";
        }
      };
    }

    @Override
    public void dispose(Principal principal) {
    }

  }

  public static class RuntimeInfoTestInjector implements Factory<RuntimeInfo> {
    @Singleton
    @Override
    public RuntimeInfo provide() {
      RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
      return runtimeInfo;
    }

    @Override
    public void dispose(RuntimeInfo runtimeInfo) {
    }

  }

  public static class RuntimeInfoTestInjectorForSlaveMode implements Factory<RuntimeInfo> {
    @Singleton
    @Override
    public RuntimeInfo provide() {
      RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
      return runtimeInfo;
    }

    @Override
    public void dispose(RuntimeInfo runtimeInfo) {
    }

  }

}
