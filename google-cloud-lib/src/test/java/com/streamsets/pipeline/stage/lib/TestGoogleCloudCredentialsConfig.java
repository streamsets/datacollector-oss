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

package com.streamsets.pipeline.stage.lib;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.stage.pubsub.origin.PubSubDSource;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.streamsets.pipeline.stage.lib.Errors.GOOGLE_01;
import static com.streamsets.pipeline.stage.lib.Errors.GOOGLE_02;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestGoogleCloudCredentialsConfig {
  @Test
  public void testJsonCredentialsNotFound() {
    GoogleCloudCredentialsConfig credentialsConfig = new GoogleCloudCredentialsConfig();
    credentialsConfig.projectId = "test";
    credentialsConfig.credentialsProvider = CredentialsProviderType.JSON_PROVIDER;
    credentialsConfig.path = "/tmp/does-not-exist.json";

    List<Stage.ConfigIssue> issues = new ArrayList<>();
    credentialsConfig.getCredentialsProvider(createContext(), issues);

    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(GOOGLE_01.getCode()));
  }

  @Test
  public void testInvalidJsonCredentials() throws Exception {
    Path tempFile = Files.createTempFile("creds", "json");
    GoogleCloudCredentialsConfig credentialsConfig = new GoogleCloudCredentialsConfig();
    credentialsConfig.projectId = "test";
    credentialsConfig.credentialsProvider = CredentialsProviderType.JSON_PROVIDER;
    credentialsConfig.path = tempFile.toString();

    List<Stage.ConfigIssue> issues = new ArrayList<>();
    credentialsConfig.getCredentialsProvider(createContext(), issues);

    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(GOOGLE_02.getCode()));
  }

  private Stage.Context createContext() {
    PushSourceRunner runner = new PushSourceRunner.Builder(PubSubDSource.class)
      .addOutputLane("lane")
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    return runner.getContext();
  }
}