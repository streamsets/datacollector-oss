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

import com.streamsets.pipeline.api.Stage;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public class TestFTPRemoteConnector extends RemoteConnectorTestBase {

  @Override
  protected void setupServer() throws Exception {
    setupFTPServer(path);
  }

  @Override
  protected FTPRemoteConnectorForTest getConnector(RemoteConfigBean bean) {
    return new FTPRemoteConnectorForTest(bean);
  }

  @Override
  protected String getScheme() {
    return "ftp";
  }

  @Test
  public void testPrivateKey() throws Exception {
    FTPRemoteConnectorForTest connector = new FTPRemoteConnectorForTest(getBean(
        getScheme() + "://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        null,
        null,
        null,
        null,
        true,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.privateKey",
        Errors.REMOTE_06
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Override
  protected String getTestWrongPassMessage() {
    return "Could not connect to FTP server on \"localhost\".";
  }

  @Override
  protected String getTestDirDoesntExistMessage(File dir) {
    return "Could not list the contents of \"" + getScheme() + "://localhost:" + port + "/" + dir.getName()
        + "\" because it is not a folder.";
  }

  protected void testVerifyAndReconnectHelper(RemoteConnector connector) throws Exception {
    FTPRemoteConnectorForTest con = (FTPRemoteConnectorForTest) connector;
    // We'll fail the first call to remoteDir.getChildren, which will cause verifyAndReconnect to replace remoteDir with
    // a new one
    con.remoteDir = Mockito.spy(con.remoteDir);
    FileObject originalRemoteDir = con.remoteDir;
    Mockito.when(con.remoteDir.getChildren()).thenThrow(new FileSystemException(""));
    connector.verifyAndReconnect();
    Assert.assertNotEquals(originalRemoteDir, con.remoteDir);
    verifyConnection(connector);
  }

  protected void stopServer() throws Exception {
    ftpServer.stop();
  }

  @Test
  public void testRelativizeToRoot() throws Exception {
    FTPRemoteConnectorForTest connector = new FTPRemoteConnectorForTest(null);

    connector.remoteDir = createFileObject("/");
    Assert.assertEquals("/foo/bar/asd", connector.relativizeToRoot("/foo/bar/asd"));

    connector.remoteDir = createFileObject("/foo");
    Assert.assertEquals("/bar/asd", connector.relativizeToRoot("/foo/bar/asd"));

    connector.remoteDir = createFileObject("/foo/bar");
    Assert.assertEquals("/asd", connector.relativizeToRoot("/foo/bar/asd"));

    connector.remoteDir = createFileObject("/foo/bar/asd");
    Assert.assertEquals("/", connector.relativizeToRoot("/foo/bar/asd"));
  }

  private FileObject createFileObject(String path) {
    FileObject fileObject = Mockito.mock(FileObject.class);
    FileName fn = Mockito.mock(FileName.class);
    Mockito.when(fn.getPath()).thenReturn(path);
    Mockito.when(fileObject.getName()).thenReturn(fn);
    return fileObject;
  }

  @Override
  protected void verifyConnection(RemoteConnector connector) throws IOException {
    FTPRemoteConnectorForTest con = (FTPRemoteConnectorForTest) connector;
    FileObject[] children = con.remoteDir.getChildren();
    Assert.assertEquals(1, children.length);
    String line = IOUtils.toString(children[0].getContent().getInputStream(), Charset.forName("UTF-8"));
    Assert.assertEquals(TEXT, line);
  }

  private class FTPRemoteConnectorForTest extends FTPRemoteConnector {
    protected FTPRemoteConnectorForTest(RemoteConfigBean remoteConfig) {
      super(remoteConfig);
    }
  }
}
