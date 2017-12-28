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
package com.streamsets.pipeline.sdk;

import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;

import com.streamsets.pipeline.api.StageDef;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestStageRunner {


  @StageDef(
    version = 1,
    label = "Test",
    onlineHelpRefUrl = ""
  )
  public static abstract class DummyStage implements Stage {
  }

  public static class DummyStageRunner extends StageRunner<DummyStage> {

    DummyStageRunner(
      Class<DummyStage> stageClass,
      Map<String, Object> configuration,
      List<String> outputLanes,
      boolean isPreview,
      Map<String, Object> constants,
      Map<String, String> stageSdcConf,
      ExecutionMode executionMode,
      String resourcesDir,
      RuntimeInfo runtimeInfo,
      List<ServiceRunner> services
    ) {
      super(
        stageClass,
        StageType.SOURCE,
        configuration,
        outputLanes,
        isPreview,
        OnRecordError.TO_ERROR,
        constants,
        stageSdcConf,
        executionMode,
        DeliveryGuarantee.AT_LEAST_ONCE,
        resourcesDir,
        runtimeInfo,
        services
      );
    }

    DummyStageRunner(
      Class<DummyStage> stageClass,
      DummyStage stage,
      Map<String, Object> configuration,
      List<String> outputLanes,
      boolean isPreview,
      Map<String, Object> constants,
      Map<String, String> stageSdcConf,
      ExecutionMode executionMode,
      String resourcesDir,
      RuntimeInfo runtimeInfo,
      List<ServiceRunner> services
    ) {
      super(
        stageClass,
        stage,
        StageType.SOURCE,
        configuration,
        outputLanes,
        isPreview,
        OnRecordError.TO_ERROR,
        constants,
        stageSdcConf,
        executionMode,
        DeliveryGuarantee.AT_LEAST_ONCE,
        resourcesDir,
        runtimeInfo,
        services
      );
    }

    public static class Builder extends StageRunner.Builder<DummyStage, DummyStageRunner, Builder> {

      public Builder(DummyStage stage) {
        super(DummyStage.class, stage);
      }

      @SuppressWarnings("unchecked")
      public Builder(Class<? extends DummyStage> stageClass) {
        super((Class<DummyStage>) stageClass);
      }

      @Override
      public DummyStageRunner build() {
        return (stage != null) ?
          new DummyStageRunner(
            stageClass,
            stage,
            configs,
            outputLanes,
            isPreview,
            constants,
            stageSdcConf,
            executionMode,
            resourcesDir,
            runtimeInfo,
            services
          )
          : new DummyStageRunner(
            stageClass,
            configs,
            outputLanes,
            isPreview,
            constants,
            stageSdcConf,
            executionMode,
            resourcesDir,
            runtimeInfo,
            services
          );
      }
    }
  }

  @StageDef(
    version = 1,
    label = "Test",
    onlineHelpRefUrl = ""
  )
  public static class DummyStage1 extends DummyStage {

    public boolean initialized;
    public boolean destroyed;

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      return Collections.emptyList();
    }

    @Override
    public void destroy() {
      destroyed = true;
    }

  }

  @Test
  public void testBuilderWithClass() {
    DummyStageRunner.Builder builder = new DummyStageRunner.Builder(DummyStage1.class);
    DummyStageRunner runner = builder.build();
    Assert.assertNotNull(runner);
    Assert.assertNotNull(runner.getContext());
    Assert.assertNotNull(runner.getInfo());
    Assert.assertNotNull(runner.getContext().getMetrics());
    Assert.assertNotNull(runner.getContext().getPipelineInfo());
    Assert.assertNotNull(runner.getInfo().getName());
    Assert.assertNotNull(runner.getInfo().getVersion());
    Assert.assertNotNull(runner.getInfo().getInstanceName());
    Assert.assertNotNull(runner.getClass());
    Assert.assertEquals(DummyStage1.class, runner.getStage().getClass());
    Assert.assertTrue(runner.getErrorRecords().isEmpty());
    Assert.assertTrue(runner.getErrors().isEmpty());
    Assert.assertTrue(runner.getEventRecords().isEmpty());
  }
  @Test
  public void testIsPreview() {
    DummyStageRunner.Builder builder = new DummyStageRunner.Builder(DummyStage1.class);
    DummyStageRunner runner = builder.build();
    Assert.assertFalse(runner.getContext().isPreview());
    builder.setPreview(true);
    runner = builder.build();
    Assert.assertTrue(runner.getContext().isPreview());

    builder = new DummyStageRunner.Builder(new DummyStage1());
    runner = builder.build();
    Assert.assertFalse(runner.getContext().isPreview());
    builder.setPreview(true);
    runner = builder.build();
    Assert.assertTrue(runner.getContext().isPreview());
  }

  @Test
  public void testIsClusterMode() {
    DummyStageRunner.Builder builder = new DummyStageRunner.Builder(DummyStage1.class);
    DummyStageRunner runner = builder.build();
    Assert.assertFalse(runner.getContext().isPreview());
    builder.setExecutionMode(ExecutionMode.CLUSTER_BATCH);
    runner = builder.build();
    Assert.assertEquals(ExecutionMode.CLUSTER_BATCH, runner.getContext().getExecutionMode());

    builder = new DummyStageRunner.Builder(new DummyStage1());
    runner = builder.build();
    Assert.assertEquals(ExecutionMode.STANDALONE, runner.getContext().getExecutionMode());
    builder.setExecutionMode(ExecutionMode.CLUSTER_YARN_STREAMING);
    runner = builder.build();
    Assert.assertEquals(ExecutionMode.CLUSTER_YARN_STREAMING, runner.getContext().getExecutionMode());
  }

  @Test
  public void testResourcesDir() {
    DummyStageRunner.Builder builder = new DummyStageRunner.Builder(DummyStage1.class);
    builder.setResourcesDir("foo");
    DummyStageRunner runner = builder.build();
    Assert.assertEquals("foo", runner.getContext().getResourcesDirectory());
  }

  @Test
  public void testBuilderWithInstance() {
    DummyStage1 stage = new DummyStage1();
    DummyStageRunner.Builder builder = new DummyStageRunner.Builder(stage);
    DummyStageRunner runner = builder.build();
    Assert.assertNotNull(runner);
    Assert.assertNotNull(runner.getContext());
    Assert.assertNotNull(runner.getInfo());
    Assert.assertNotNull(runner.getContext().getMetrics());
    Assert.assertNotNull(runner.getContext().getPipelineInfo());
    Assert.assertNotNull(runner.getInfo().getName());
    Assert.assertNotNull(runner.getInfo().getVersion());
    Assert.assertNotNull(runner.getInfo().getInstanceName());
    Assert.assertNotNull(runner.getClass());
    Assert.assertEquals(stage, runner.getStage());
    Assert.assertTrue(runner.getErrorRecords().isEmpty());
    Assert.assertTrue(runner.getErrors().isEmpty());
    Assert.assertTrue(runner.getEventRecords().isEmpty());
  }

  @Test
  public void testBuilderInitDestroy() throws Exception {
    DummyStage1 stage = new DummyStage1();
    DummyStageRunner.Builder builder = new DummyStageRunner.Builder(stage);
    DummyStageRunner runner = builder.build();
    runner.runInit();
    runner.runDestroy();
  }

  @Test(expected = RuntimeException.class)
  public void testBuilderInvalidInit1() throws Exception {
    DummyStage1 stage = new DummyStage1();
    DummyStageRunner.Builder builder = new DummyStageRunner.Builder(stage);
    DummyStageRunner runner = builder.build();
    runner.runInit();
    runner.runInit();
  }

  @Test(expected = RuntimeException.class)
  public void testBuilderInvalidInit2() throws Exception {
    DummyStage1 stage = new DummyStage1();
    DummyStageRunner.Builder builder = new DummyStageRunner.Builder(stage);
    DummyStageRunner runner = builder.build();
    runner.runInit();
    runner.runDestroy();
    runner.runInit();
  }

  @Test(expected = RuntimeException.class)
  public void testBuilderInvalidDestroy1() throws Exception {
    DummyStage1 stage = new DummyStage1();
    DummyStageRunner.Builder builder = new DummyStageRunner.Builder(stage);
    DummyStageRunner runner = builder.build();
    runner.runDestroy();
  }

  @Test(expected = RuntimeException.class)
  public void testBuilderInvalidDestroy2() throws Exception {
    DummyStage1 stage = new DummyStage1();
    DummyStageRunner.Builder builder = new DummyStageRunner.Builder(stage);
    DummyStageRunner runner = builder.build();
    runner.runInit();
    runner.runDestroy();
    runner.runDestroy();
  }

  @Test(expected = RuntimeException.class)
  public void testBuilderWithInvalidConfig() {
    DummyStageRunner.Builder builder = new DummyStageRunner.Builder(DummyStage1.class);
    builder.addConfiguration("a", Boolean.TRUE);
    builder.build();
  }

  @StageDef(
    version = 1,
    label = "Test",
    onlineHelpRefUrl = ""
  )
  public static class DummyStage2 extends DummyStage1 {

    @ConfigDef(type = ConfigDef.Type.BOOLEAN, label = "L", required = false)
    public boolean a;

  }

  @Test(expected = RuntimeException.class)
  public void testBuilderWithMissingConfig() {
    DummyStageRunner.Builder builder = new DummyStageRunner.Builder(DummyStage2.class);
    builder.build();
  }

  @Test
  public void testBuilderWithValidConfig() {
    DummyStageRunner.Builder builder = new DummyStageRunner.Builder(DummyStage2.class);
    builder.addConfiguration("a", Boolean.TRUE);
    DummyStage stage = builder.build().getStage();
    Assert.assertEquals(true, ((DummyStage2)stage).a);
  }

}
