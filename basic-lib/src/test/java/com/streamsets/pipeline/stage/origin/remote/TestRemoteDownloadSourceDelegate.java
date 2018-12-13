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
package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
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
import java.util.Map;
import java.util.NavigableSet;

public class TestRemoteDownloadSourceDelegate {

  @Test
  public void testResolveCredential() throws Exception {
    CredentialValue cred = Mockito.mock(CredentialValue.class);
    Mockito.when(cred.get()).thenReturn("bar");
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    Source.Context context = Mockito.mock(Source.Context.class);
    RemoteDownloadConfigBean conf = new RemoteDownloadConfigBean();
    RemoteDownloadSourceDelegate delegate = createRemoteDownloadSourceDelegate(conf);

    String resolved = delegate.resolveCredential(cred, "foo", issues, context);
    Assert.assertEquals("bar", resolved);
    assertNumIssues(issues, 0);
  }

  @Test
  public void testResolveCredentialIssue() throws Exception {
    CredentialValue cred = Mockito.mock(CredentialValue.class);
    Mockito.when(cred.get()).thenThrow(new StageException(Errors.REMOTE_17, "foo"));
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    Source.Context context = Mockito.mock(Source.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenAnswer(new Answer<ConfigIssue>() {
      @Override
      public ConfigIssue answer(InvocationOnMock invocation) throws Throwable {
        String label = invocation.getArgumentAt(0, String.class);
        Assert.assertEquals(Groups.CREDENTIALS.getLabel(), label);
        Errors error = invocation.getArgumentAt(2, Errors.class);
        Assert.assertEquals(Errors.REMOTE_17, error);

        String message = invocation.getArgumentAt(3, String.class);
        ConfigIssue configIssue = Mockito.mock(ConfigIssue.class);
        Mockito.when(configIssue.toString()).thenReturn(message);
        return configIssue;
      }
    });
    RemoteDownloadConfigBean conf = new RemoteDownloadConfigBean();
    RemoteDownloadSourceDelegate delegate = createRemoteDownloadSourceDelegate(conf);

    String resolved = delegate.resolveCredential(cred, "foo", issues, context);
    Assert.assertNull(resolved);
    assertNumIssues(issues, 1);
    Assert.assertTrue(issues.get(0).toString().contains("foo"));
  }

  @Test
  public void testResolveUsername() throws Exception {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    Source.Context context = Mockito.mock(Source.Context.class);
    RemoteDownloadConfigBean conf = new RemoteDownloadConfigBean();
    CredentialValue cred = Mockito.mock(CredentialValue.class);
    Mockito.when(cred.get()).thenReturn("user3");
    conf.username = cred;

    URI uri = URI.create("sftp://user1:pass@host");
    RemoteDownloadSourceDelegate delegate = createRemoteDownloadSourceDelegate(conf);
    String resolved = delegate.resolveUsername(uri, issues, context);
    Assert.assertEquals("user1", resolved);
    Mockito.verify(delegate,
        Mockito.never()).resolveCredential(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    assertNumIssues(issues, 0);

    uri = URI.create("sftp://user2@host");
    resolved = delegate.resolveUsername(uri, issues, context);
    Assert.assertEquals("user2", resolved);
    Mockito.verify(delegate,
        Mockito.never()).resolveCredential(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    assertNumIssues(issues, 0);

    uri = URI.create("sftp://host");
    resolved = delegate.resolveUsername(uri, issues, context);
    Assert.assertEquals("user3", resolved);
    Mockito.verify(delegate, Mockito.times(1))
        .resolveCredential(cred, RemoteDownloadSourceDelegate.CONF_PREFIX + "username", issues, context);
    assertNumIssues(issues, 0);
  }

  @Test
  public void testResolvePassword() throws Exception {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    Source.Context context = Mockito.mock(Source.Context.class);
    RemoteDownloadConfigBean conf = new RemoteDownloadConfigBean();
    CredentialValue cred = Mockito.mock(CredentialValue.class);
    Mockito.when(cred.get()).thenReturn("pass2");
    conf.password = cred;

    URI uri = URI.create("sftp://user:pass1@host");
    RemoteDownloadSourceDelegate delegate = createRemoteDownloadSourceDelegate(conf);
    String resolved = delegate.resolvePassword(uri, issues, context);
    Assert.assertEquals("pass1", resolved);
    Mockito.verify(delegate,
        Mockito.never()).resolveCredential(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    assertNumIssues(issues, 0);

    uri = URI.create("sftp://user@host");
    delegate = createRemoteDownloadSourceDelegate(conf);
    resolved = delegate.resolvePassword(uri, issues, context);
    Assert.assertEquals("pass2", resolved);
    Mockito.verify(delegate, Mockito.times(1))
        .resolveCredential(cred, RemoteDownloadSourceDelegate.CONF_PREFIX + "password", issues, context);
    assertNumIssues(issues, 0);

    uri = URI.create("sftp://host");
    delegate = createRemoteDownloadSourceDelegate(conf);
    resolved = delegate.resolvePassword(uri, issues, context);
    Assert.assertEquals("pass2", resolved);
    Mockito.verify(delegate, Mockito.times(1))
        .resolveCredential(cred, RemoteDownloadSourceDelegate.CONF_PREFIX + "password", issues, context);
    assertNumIssues(issues, 0);
  }

  private RemoteDownloadSourceDelegate createRemoteDownloadSourceDelegate(RemoteDownloadConfigBean conf) {
    RemoteDownloadSourceDelegate delegate = new RemoteDownloadSourceDelegate(conf) {

      @Override
      protected void initAndConnectInternal(
          List<Stage.ConfigIssue> issues, Source.Context context, URI remoteURI
      ) throws IOException {

      }

      @Override
      Offset createOffset(String file) throws IOException {
        return null;
      }

      @Override
      long populateMetadata(String remotePath, Map<String, Object> metadata) throws IOException {
        return 0;
      }

      @Override
      void queueFiles(
          FileQueueChecker fqc, NavigableSet<RemoteFile> fileQueue, FileFilter fileFilter
      ) throws IOException, StageException {

      }

      @Override
      void close() throws IOException {

      }
    };
    return Mockito.spy(delegate);
  }

  private void assertNumIssues(List<Stage.ConfigIssue> issues, int num) {
    Assert.assertEquals("Expected " + num + " issues but found " + issues.size() + ": " + Arrays.toString(issues.toArray()),
        num, issues.size());
  }
}
