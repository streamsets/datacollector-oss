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

import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.StageBean;
import com.streamsets.datacollector.creation.StageConfigBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.CreateByRef;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.Callable;

public class TestStageRuntime {

  @Test
  public void testCreateByRef() throws Exception {
    PipelineBean pipelineBean = Mockito.mock(PipelineBean.class);
    StageBean stageBean = Mockito.mock(StageBean.class);
    Mockito.when(stageBean.getSystemConfigs()).thenReturn(new StageConfigBean());
    StageConfiguration conf = Mockito.mock(StageConfiguration.class);
    Mockito.when(conf.getUiInfo()).thenReturn(Collections.emptyMap());
    Mockito.when(stageBean.getConfiguration()).thenReturn(conf);
    StageDefinition def = Mockito.mock(StageDefinition.class);
    Mockito.when(stageBean.getDefinition()).thenReturn(def);
    StageContext context = Mockito.mock(StageContext.class);
    StageRuntime runtime = new StageRuntime(pipelineBean, stageBean);
    runtime.setContext(context);

    // by value, no preview
    Mockito.when(def.getRecordsByRef()).thenReturn(false);
    Mockito.when(context.isPreview()).thenReturn(false);
    runtime.execute(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Assert.assertFalse(CreateByRef.isByRef());
        return null;
      }
    }, null, null);

    // by value, preview
    Mockito.when(def.getRecordsByRef()).thenReturn(false);
    Mockito.when(context.isPreview()).thenReturn(true);
    runtime.execute(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Assert.assertFalse(CreateByRef.isByRef());
        return null;
      }
  }, null, null);

    // by ref, no preview
    Mockito.when(def.getRecordsByRef()).thenReturn(true);
    Mockito.when(context.isPreview()).thenReturn(false);
    runtime.execute(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Assert.assertTrue(CreateByRef.isByRef());
        return null;
      }
  }, null, null);

    // by ref, preview
    Mockito.when(def.getRecordsByRef()).thenReturn(true);
    Mockito.when(context.isPreview()).thenReturn(true);
    runtime.execute(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Assert.assertFalse(CreateByRef.isByRef());
        return null;
      }
  }, null, null);

  }


  @Test
  public void testReleaseClassLoader() throws Exception {
    PipelineBean pipelineBean = Mockito.mock(PipelineBean.class);
    StageBean stageBean = Mockito.mock(StageBean.class);
    StageConfiguration conf = Mockito.mock(StageConfiguration.class);
    Mockito.when(conf.getUiInfo()).thenReturn(Collections.emptyMap());
    Mockito.when(stageBean.getConfiguration()).thenReturn(conf);
    Mockito.when(stageBean.getStage()).thenReturn(Mockito.mock(Stage.class));
    StageDefinition def = Mockito.mock(StageDefinition.class);
    Mockito.when(stageBean.getDefinition()).thenReturn(def);
    StageContext context = Mockito.mock(StageContext.class);

    StageRuntime runtime = new StageRuntime(pipelineBean, stageBean);
    runtime.setContext(context);

    Mockito.verify(stageBean, Mockito.never()).releaseClassLoader();
    runtime.destroy(new ErrorSink(), new EventSink());
    Mockito.verify(stageBean, Mockito.times(1)).releaseClassLoader();
  }

  }
