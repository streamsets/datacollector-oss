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

package com.streamsets.pipeline.stage.pubsub.origin;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.stage.lib.CredentialsProviderType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.streamsets.pipeline.stage.lib.Errors.GOOGLE_01;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPubSubSource {
  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testInvalidDefaultCredentials() throws Exception {
    PubSubSourceConfig config = getConfig();

    PubSubSource source = new PubSubSource(config);
    PushSourceRunner runner = new PushSourceRunner.Builder(PubSubDSource.class, source)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("The Application Default Credentials are not available"));
  }

  @Test
  public void testJsonCredentialsNotFound() throws Exception {
    PubSubSourceConfig config = getConfig();
    config.credentials.credentialsProvider = CredentialsProviderType.JSON_PROVIDER;
    config.credentials.path = "/tmp/does_not_exist.json";

    PubSubSource source = new PubSubSource(config);
    PushSourceRunner runner = new PushSourceRunner.Builder(PubSubDSource.class, source)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(GOOGLE_01.name()));
  }

  private PubSubSourceConfig getConfig() {
    PubSubSourceConfig config = new PubSubSourceConfig();
    config.basic.maxWaitTime = 1000;
    config.basic.maxBatchSize = 2;
    config.credentials.credentialsProvider = CredentialsProviderType.DEFAULT_PROVIDER;
    config.credentials.projectId = "test";
    config.maxThreads = 1;
    config.subscriptionId = "test";
    config.dataFormat = DataFormat.TEXT;

    return config;
  }
}