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

package com.streamsets.pipeline.stage.origin.eventhubs;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestEventHubConsumerSource {

  @Test
  public void testInvalidConf() throws Exception {
    EventHubConsumerSource eventHubConsumerSource = new EventHubConsumerSourceBuilder()
        .namespaceName("inValidNamspaceName")
        .eventHubName("inValidSampleEventHub")
        .sasKeyName("inValidSasKeyName")
        .sasKey("inValidSasKey")
        .build();

    PushSourceRunner sourceRunner = new PushSourceRunner
        .Builder(EventHubConsumerDSource.class, eventHubConsumerSource)
        .addOutputLane("a").build();

    List<Stage.ConfigIssue> issues = sourceRunner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

}
