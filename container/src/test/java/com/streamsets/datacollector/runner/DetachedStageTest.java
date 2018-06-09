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
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DetachedStageTest {

  @Test
  public void createProcessor() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    StageConfiguration stageConf = MockStages.createProcessor("p", Collections.emptyList(), Collections.emptyList());
    List<Issue> issues = new ArrayList<>();

    DetachedStageRuntime stageRuntime = DetachedStage.get().createDetachedStage(
      stageConf,
      lib,
      "pipelineId",
      "pipelineTitle",
      "rev",
      Mockito.mock(UserContext.class),
      Mockito.mock(MetricRegistry.class),
      0,
      ExecutionMode.STANDALONE,
      DeliveryGuarantee.AT_LEAST_ONCE,
      Mockito.mock(RuntimeInfo.class),
      Mockito.mock(EmailSender.class),
      Mockito.mock(Configuration.class),
      0,
      new LineagePublisherDelegator.NoopDelegator(),
      issues
    );

    assertNotNull(stageRuntime);
    assertEquals(0, issues.size());
  }
}
