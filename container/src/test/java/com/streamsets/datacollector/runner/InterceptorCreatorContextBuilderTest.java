/*
 * Copyright 2019 StreamSets Inc.
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
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.BlobStore;
import com.streamsets.pipeline.api.interceptor.InterceptorCreator;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.mock;

public class InterceptorCreatorContextBuilderTest {

  private static BlobStore blobStore = mock(BlobStore.class);
  private static Configuration configuration = mock(Configuration.class);

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicateInterceptorConfiguration() {
    PipelineStartEvent.InterceptorConfiguration conf = new PipelineStartEvent.InterceptorConfiguration();
    conf.setInterceptorClassName("com.interceptor");
    conf.setInterceptorClassName("stage-lib");

    new InterceptorCreatorContextBuilder(blobStore, configuration, ImmutableList.of(conf, conf));
  }

  @Test
  public void testBuildContexts() {
    PipelineStartEvent.InterceptorConfiguration conf1 = new PipelineStartEvent.InterceptorConfiguration();
    conf1.setInterceptorClassName("com.interceptor");
    conf1.setStageLibrary("stage-lib");
    conf1.setParameters(Collections.singletonMap("a", "b"));
    PipelineStartEvent.InterceptorConfiguration conf2 = new PipelineStartEvent.InterceptorConfiguration();
    conf2.setInterceptorClassName("org.inject");
    conf2.setStageLibrary("interceptor-lib");
    conf2.setParameters(Collections.singletonMap("1", "2"));

    InterceptorCreatorContextBuilder builder = new InterceptorCreatorContextBuilder(blobStore, configuration, ImmutableList.of(conf1, conf2));

    InterceptorCreator.BaseContext baseContext = builder.buildBaseContext("stage-lib", "com.interceptor");
    assertNotNull(baseContext);
    assertNotNull(baseContext.getParameters());
    assertEquals("b", baseContext.getParameters().get("a"));

    InterceptorCreator.Context context = builder.buildFor("interceptor-lib", "org.inject", null, mock(StageDefinition.class), null);
    assertNotNull(context);
    assertNotNull(context.getParameters());
    assertEquals("2", context.getParameters().get("1"));
  }
}
