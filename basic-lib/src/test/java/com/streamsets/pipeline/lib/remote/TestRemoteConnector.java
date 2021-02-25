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
package com.streamsets.pipeline.lib.remote;

import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.ConfigIssueContext;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.stage.connection.remote.RemoteConnection;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestRemoteConnector {

  @Test
  public void testGetURI() throws Exception {
    // basic case
    testGetURI("sftp", null, "host", 42, 42);
    testGetURI("ftp", null, "host", 1001, 1001);

    // adds default ports
    testGetURI("sftp", null, "host", null, 22);
    testGetURI("ftp", null, "host", null, 21);

    // various user info
    String[] userInfos = new String[]{
        "user",
        "user:pass",
        "user@email.com",
        "user@email.com:pass",
        "user:p@ss",
        "user@email.com:p@ss",
        "@",
        "@:@",
        "@@:@@",
    };
    for (String userInfo : userInfos) {
      testGetURI("sftp", userInfo, "host", 42, 42);
    }
  }

  private void testGetURI(
      String scheme,
      String userInfo,
      String host,
      Integer port,
      int expectedPort
  ) throws Exception {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    RemoteConfigBean conf = new RemoteConfigBean();
    conf.connection = new RemoteConnection();
    conf.connection.remoteAddress =
        scheme + "://" +
        (userInfo == null ? "" : userInfo + "@") +
        host +
        (port == null ? "" : ":" + port);
    URI uri = RemoteConnector.getURI(conf, issues, null, null);
    URI expectedURI = new URI(scheme, userInfo, host, expectedPort, null, null, null);
    Assert.assertEquals(expectedURI, uri);
    Assert.assertEquals(scheme, uri.getScheme());
    Assert.assertEquals(host, uri.getHost());
    Assert.assertEquals(userInfo, uri.getUserInfo());
    Assert.assertEquals(expectedPort, uri.getPort());
    assertNumIssues(issues, 0);
  }

  @Test
  public void testGetURIInvalid() throws Exception {
    testGetURIInvalid("sftp://?!?!$A*( W:80", Errors.REMOTE_01);  // Invalid characters in URI
    testGetURIInvalid("http://host:80", Errors.REMOTE_02);        // wrong scheme
    testGetURIInvalid("host:80", Errors.REMOTE_02);               // no scheme
  }

  public void testGetURIInvalid(String address, Errors exepctedError) throws Exception {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    Label group = Mockito.mock(Label.class);
    Mockito.when(group.getLabel()).thenReturn("REMOTE");
    ConfigIssueContext context = Mockito.mock(ConfigIssueContext.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenAnswer(new Answer<ConfigIssue>() {
          @Override
          public ConfigIssue answer(InvocationOnMock invocation) throws Throwable {
            String label = invocation.getArgumentAt(0, String.class);
            Assert.assertEquals(group.getLabel(), label);
            Errors error = invocation.getArgumentAt(2, Errors.class);
            Assert.assertEquals(exepctedError, error);

            String message = invocation.getArgumentAt(3, String.class);
            ConfigIssue configIssue = Mockito.mock(ConfigIssue.class);
            Mockito.when(configIssue.toString()).thenReturn(message);
            return configIssue;
          }
        });
    RemoteConfigBean conf = new RemoteConfigBean();
    conf.connection = new RemoteConnection();
    conf.connection.remoteAddress = address;
    URI uri = RemoteConnector.getURI(conf, issues, context, group);
    Assert.assertNull(uri);
    assertNumIssues(issues, 1);
  }

  @Test
  public void testResolveCredential() throws Exception {
    Label group = Mockito.mock(Label.class);
    Mockito.when(group.getLabel()).thenReturn("CREDENTIALS");
    CredentialValue cred = Mockito.mock(CredentialValue.class);
    Mockito.when(cred.get()).thenReturn("bar");
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    ConfigIssueContext context = Mockito.mock(ConfigIssueContext.class);
    RemoteConfigBean conf = new RemoteConfigBean();
    RemoteConnector connector = createRemoteConnector(conf);

    String resolved = connector.resolveCredential(cred, "foo", issues, context, group);
    Assert.assertEquals("bar", resolved);
    assertNumIssues(issues, 0);
  }

  @Test
  public void testResolveCredentialIssue() throws Exception {
    Label group = Mockito.mock(Label.class);
    Mockito.when(group.getLabel()).thenReturn("CREDENTIALS");
    CredentialValue cred = Mockito.mock(CredentialValue.class);
    Mockito.when(cred.get()).thenThrow(new StageException(Errors.REMOTE_08, "foo"));
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    ConfigIssueContext context = Mockito.mock(ConfigIssueContext.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenAnswer(new Answer<ConfigIssue>() {
          @Override
          public ConfigIssue answer(InvocationOnMock invocation) throws Throwable {
            String label = invocation.getArgumentAt(0, String.class);
            Assert.assertEquals(group.getLabel(), label);
            Errors error = invocation.getArgumentAt(2, Errors.class);
            Assert.assertEquals(Errors.REMOTE_08, error);

            String message = invocation.getArgumentAt(3, String.class);
            ConfigIssue configIssue = Mockito.mock(ConfigIssue.class);
            Mockito.when(configIssue.toString()).thenReturn(message);
            return configIssue;
          }
        });
    RemoteConfigBean conf = new RemoteConfigBean();
    RemoteConnector connector = createRemoteConnector(conf);

    String resolved = connector.resolveCredential(cred, "foo", issues, context, group);
    Assert.assertNull(resolved);
    assertNumIssues(issues, 1);
    Assert.assertTrue(issues.get(0).toString().contains("foo"));
  }

  @Test
  public void testResolveUsername() throws Exception {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    Label group = Mockito.mock(Label.class);
    Mockito.when(group.getLabel()).thenReturn("CREDENTIALS");
    ConfigIssueContext context = Mockito.mock(ConfigIssueContext.class);
    RemoteConfigBean conf = new RemoteConfigBean();
    conf.connection = new RemoteConnection();
    CredentialValue cred = Mockito.mock(CredentialValue.class);
    Mockito.when(cred.get()).thenReturn("user3");
    conf.connection.credentials.username = cred;

    URI uri = URI.create("sftp://user1:pass@host");
    RemoteConnector connector = createRemoteConnector(conf);
    String resolved = connector.resolveUsername(uri, issues, context, group);
    Assert.assertEquals("user1", resolved);
    Mockito.verify(connector,
        Mockito.never()).resolveCredential(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    assertNumIssues(issues, 0);

    uri = URI.create("sftp://user2@host");
    resolved = connector.resolveUsername(uri, issues, context, group);
    Assert.assertEquals("user2", resolved);
    Mockito.verify(connector,
        Mockito.never()).resolveCredential(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    assertNumIssues(issues, 0);

    uri = URI.create("sftp://host");
    resolved = connector.resolveUsername(uri, issues, context, group);
    Assert.assertEquals("user3", resolved);
    Mockito.verify(connector, Mockito.times(1))
        .resolveCredential(cred, "conf.remoteConfig.username", issues, context, group);
    assertNumIssues(issues, 0);
  }

  @Test
  public void testResolvePassword() throws Exception {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    Label group = Mockito.mock(Label.class);
    Mockito.when(group.getLabel()).thenReturn("CREDENTIALS");
    ConfigIssueContext context = Mockito.mock(ConfigIssueContext.class);
    RemoteConfigBean conf = new RemoteConfigBean();
    conf.connection = new RemoteConnection();
    CredentialValue cred = Mockito.mock(CredentialValue.class);
    Mockito.when(cred.get()).thenReturn("pass2");
    conf.connection.credentials.password = cred;

    URI uri = URI.create("sftp://user:pass1@host");
    RemoteConnector connector = createRemoteConnector(conf);
    String resolved = connector.resolvePassword(uri, issues, context, group);
    Assert.assertEquals("pass1", resolved);
    Mockito.verify(connector,
        Mockito.never()).resolveCredential(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    assertNumIssues(issues, 0);

    uri = URI.create("sftp://user@host");
    connector = createRemoteConnector(conf);
    resolved = connector.resolvePassword(uri, issues, context, group);
    Assert.assertEquals("pass2", resolved);
    Mockito.verify(connector, Mockito.times(1))
        .resolveCredential(cred, "conf.remoteConfig.password", issues, context, group);
    assertNumIssues(issues, 0);

    uri = URI.create("sftp://host");
    connector = createRemoteConnector(conf);
    resolved = connector.resolvePassword(uri, issues, context, group);
    Assert.assertEquals("pass2", resolved);
    Mockito.verify(connector, Mockito.times(1))
        .resolveCredential(cred, "conf.remoteConfig.password", issues, context, group);
    assertNumIssues(issues, 0);
  }

  private RemoteConnector createRemoteConnector(RemoteConfigBean conf) {
    RemoteConnector connector = new RemoteConnector(conf) {
      @Override
      protected void initAndConnect(
          List<Stage.ConfigIssue> issues, ConfigIssueContext context, URI remoteURI, Label remoteGroup, Label credGroup
      ) {
      }

      @Override
      public void verifyAndReconnect() throws StageException {
      }

      @Override
      public void close() throws IOException {
      }
    };
    return Mockito.spy(connector);
  }

  private void assertNumIssues(List<Stage.ConfigIssue> issues, int num) {
    Assert.assertEquals(
        "Expected " + num + " issues but found " + issues.size() + ": " + Arrays.toString(issues.toArray()),
        num,
        issues.size()
    );
  }
}
