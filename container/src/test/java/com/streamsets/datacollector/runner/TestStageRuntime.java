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
package com.streamsets.datacollector.runner;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.StageBean;
import com.streamsets.datacollector.creation.StageConfigBean;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.CreateByRef;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class TestStageRuntime {
  private PipelineBean pipelineBean;
  private StageBean stageBean;
  private Stage stage;
  private StageConfiguration conf;
  private StageDefinition def;
  private StageContext context;
  private StageRuntime runtime;
  private ServiceRuntime serviceRuntime;

  @Before
  public void setUp() {
    this.pipelineBean = Mockito.mock(PipelineBean.class);

    this.stageBean = Mockito.mock(StageBean.class);
    Mockito.when(stageBean.getSystemConfigs()).thenReturn(new StageConfigBean());

    this.conf = Mockito.mock(StageConfiguration.class);
    Mockito.when(conf.getUiInfo()).thenReturn(Collections.emptyMap());
    Mockito.when(stageBean.getConfiguration()).thenReturn(conf);

    this.def = Mockito.mock(StageDefinition.class);
    Mockito.when(stageBean.getDefinition()).thenReturn(def);

    this.stage = Mockito.mock(Stage.class);
    Mockito.when(stageBean.getStage()).thenReturn(stage);

    this.context = Mockito.mock(StageContext.class);

    this.serviceRuntime = Mockito.mock(ServiceRuntime.class);

    this.runtime = new StageRuntime(pipelineBean, stageBean, ImmutableList.of(serviceRuntime));
    runtime.setContext(context);
  }

  @Test
  public void testCreateByRef() throws Exception {
    // by value, no preview
    Mockito.when(def.getRecordsByRef()).thenReturn(false);
    Mockito.when(context.isPreview()).thenReturn(false);
    runtime.execute(() -> {
      Assert.assertFalse(CreateByRef.isByRef());
      return null;
    }, null, null);

    // by value, preview
    Mockito.when(def.getRecordsByRef()).thenReturn(false);
    Mockito.when(context.isPreview()).thenReturn(true);
    runtime.execute(() -> {
      Assert.assertFalse(CreateByRef.isByRef());
      return null;
    }, null, null);

    // by ref, no preview
    Mockito.when(def.getRecordsByRef()).thenReturn(true);
    Mockito.when(context.isPreview()).thenReturn(false);
    runtime.execute(() -> {
      Assert.assertTrue(CreateByRef.isByRef());
      return null;
    }, null, null);

    // by ref, preview
    Mockito.when(def.getRecordsByRef()).thenReturn(true);
    Mockito.when(context.isPreview()).thenReturn(true);
    runtime.execute(() -> {
      Assert.assertFalse(CreateByRef.isByRef());
      return null;
    }, null, null);
  }

  @Test
  public void testReleaseClassLoader() throws Exception {
    Mockito.verify(stageBean, Mockito.never()).releaseClassLoader();
    runtime.destroy(new ErrorSink(), new EventSink());
    Mockito.verify(stageBean, Mockito.times(1)).releaseClassLoader();
  }

  @Test
  public void testServiceInitialization() throws Exception {
    Mockito.verify(serviceRuntime, Mockito.never()).init();
    Mockito.verify(stage, Mockito.never()).init(Mockito.any(), Mockito.any());
    runtime.init();
    Mockito.verify(serviceRuntime, Mockito.times(1)).init();
    Mockito.verify(stage, Mockito.times(1)).init(Mockito.any(), Mockito.any());
  }

  @Test
  public void testNoStageInitOnServiceInitFailure() throws Exception {
    Mockito.verify(stage, Mockito.never()).init(Mockito.any(), Mockito.any());
    Mockito.when(serviceRuntime.init()).thenReturn(ImmutableList.of(Mockito.mock(Issue.class)));
    runtime.init();
    Mockito.verify(stage, Mockito.never()).init(Mockito.any(), Mockito.any());
  }
}
