/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestStageRunner {

  public static interface DummyStage extends Stage {
  }

  public static class DummyStageRunner extends StageRunner<DummyStage> {

    DummyStageRunner(Class<DummyStage> stageClass, Map<String, Object> configuration, List<String> outputLanes) {
      super(stageClass, configuration, outputLanes);
    }

    DummyStageRunner(DummyStage stage, Map<String, Object> configuration, List<String> outputLanes) {
      super(stage, configuration, outputLanes);
    }

    public static class Builder extends StageRunner.Builder<DummyStage, DummyStageRunner, Builder> {

      public Builder(DummyStage stage) {
        super(stage);
      }

      @SuppressWarnings("unchecked")
      public Builder(Class<? extends DummyStage> stageClass) {
        super((Class<DummyStage>) stageClass);
      }

      @Override
      public DummyStageRunner build() {
        return (stage != null) ? new DummyStageRunner(stage, configs, outputLanes)
                               : new DummyStageRunner(stageClass, configs, outputLanes);
      }
    }
  }

  public static class DummyStage1 implements DummyStage {
    public boolean initialized;
    public boolean destroyed;

    @Override
    public List<ConfigIssue> validateConfigs(Info info, Stage.Context context) {
      return Collections.emptyList();
    }

    @Override
    public void init(Info info, Context context) throws StageException {
      initialized = true;
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
