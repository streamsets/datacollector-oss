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

package com.streamsets.pipeline.lib.googlecloud;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.stage.common.CredentialsProviderType;
import com.streamsets.pipeline.stage.pubsub.origin.PubSubDSource;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.streamsets.pipeline.lib.googlecloud.Errors.GOOGLE_01;
import static com.streamsets.pipeline.lib.googlecloud.Errors.GOOGLE_02;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.when;

public class TestGoogleCloudCredentialsConfig {
  @Test
  public void testJsonCredentialsNotFound() {
    DataProcCredentialsConfig credentialsConfig = new DataProcCredentialsConfig();
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
    DataProcCredentialsConfig credentialsConfig = new DataProcCredentialsConfig();
    credentialsConfig.projectId = "test";
    credentialsConfig.credentialsProvider = CredentialsProviderType.JSON_PROVIDER;
    credentialsConfig.path = tempFile.toString();

    List<Stage.ConfigIssue> issues = new ArrayList<>();
    credentialsConfig.getCredentialsProvider(createContext(), issues);

    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(GOOGLE_02.getCode()));
  }

  @Test
  public void testGetCredentialInputStream_CredentialsFile() throws Exception {
    Path tempFile = Files.createTempFile("creds", "json");
    DataProcCredentialsConfig credentialsConfig = new DataProcCredentialsConfig();
    credentialsConfig.projectId = "test";
    credentialsConfig.credentialsProvider = CredentialsProviderType.JSON_PROVIDER;
    credentialsConfig.path = tempFile.toString();

    InputStream in = credentialsConfig.getCredentialsInputStream(createContext(), new ArrayList<>());

    assertTrue(in instanceof FileInputStream);

    Path invalidPath = Mockito.mock(Path.class);
    credentialsConfig.path = invalidPath.toString();

    in = credentialsConfig.getCredentialsInputStream(createContext(), new ArrayList<>());
    assertNull(in);
  }

  @Test
  public void testGetCredentialInputStream_CredentialsJSONContent() throws Exception {
    CredentialValue cred = Mockito.mock(CredentialValue.class);

    when(cred.get()).thenReturn("{JSON}");

    DataProcCredentialsConfig credentialsConfig = new DataProcCredentialsConfig();
    credentialsConfig.projectId = "test";
    credentialsConfig.credentialsProvider = CredentialsProviderType.JSON;
    credentialsConfig.credentialsFileContent = cred;

    InputStream in = credentialsConfig.getCredentialsInputStream(createContext(), new ArrayList<>());
    assertTrue(in instanceof ByteArrayInputStream);

    when(cred.get()).thenReturn("");

    in = credentialsConfig.getCredentialsInputStream(createContext(), new ArrayList<>());
    assertNull(in);
  }

  private Stage.Context createContext() {
    PushSourceRunner runner = new PushSourceRunner.Builder(PubSubDSource.class)
      .addOutputLane("lane")
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    return runner.getContext();
  }
}