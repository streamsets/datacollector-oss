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

package com.streamsets.pipeline.stage.destination.eventhubs;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestEventHubProducerTarget {

  @Test
  public void testInvalidConf() throws Exception {
    EventHubProducerTarget target = new EventHubProducerTargetBuilder()
        .namespaceName("inValidNamspaceName")
        .eventHubName("inValidSampleEventHub")
        .sasKeyName("inValidSasKeyName")
        .sasKey("inValidSasKey")
        .build();

    TargetRunner targetRunner = new TargetRunner
        .Builder(EventHubProducerDTarget.class, target)
        .build();

    List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

}
