/*
 * Copyright 2018 StreamSets Inc.
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

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.config.DetachedStageConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.ProductBuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.DetachedStageConfigurationJson;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.interceptor.InterceptorCreator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class InterceptorContextTest {

  private Configuration configuration;
  private InterceptorContext context;

  @Before
  public void setUp() {
    MockStages.resetStageCaptures();
    this.configuration = new Configuration();

    this.context = new InterceptorContext(
      InterceptorCreator.InterceptorType.PRE_STAGE,
      null,
      configuration,
      "stageInstance",
      "metricName",
      MockStages.createStageLibrary(),
      "pipelineId",
      "pipelineTitle",
      "rev",
      "sdcId",
      false,
      Mockito.mock(UserContext.class),
      Mockito.mock(MetricRegistry.class),
      ExecutionMode.STANDALONE,
      DeliveryGuarantee.AT_LEAST_ONCE,
      ProductBuildInfo.getDefault(),
      Mockito.mock(RuntimeInfo.class),
      Mockito.mock(EmailSender.class),
      0,
      Mockito.mock(LineagePublisherDelegator.class)
    );
  }

  @Test
  public void testCreateStageGuardAllowed() throws Exception {
    context.setAllowCreateStage(true);
    assertNull(context.createStage(null, Processor.class));
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateStageGuardNotAllowed() throws Exception {
    context.setAllowCreateStage(false);
    context.createStage(null, Processor.class);
  }

  @Test
  public void testCreateStage() throws Exception {
    // Create mocked JSON stage
    StageConfiguration stageConf = MockStages.createProcessor("p", Collections.emptyList(), Collections.emptyList());
    DetachedStageConfiguration detachedStage = new DetachedStageConfiguration(stageConf);
    DetachedStageConfigurationJson stageConfigurationJson = new DetachedStageConfigurationJson(detachedStage);

    String json = ObjectMapperFactory.get().writeValueAsString(stageConfigurationJson);

    context.setAllowCreateStage(true);
    Processor processor = context.createStage(json, Processor.class);

    assertNotNull(processor);
    assertEquals(0, context.getIssues().size());
    assertEquals(1, context.getStageRuntimes().size());
    assertEquals(processor, context.getStageRuntimes().get(0));
  }
}
