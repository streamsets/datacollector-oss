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

import com.google.common.io.Files;
import com.streamsets.pipeline.api.Stage;
import net.schmizz.sshj.SSHClient;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static java.lang.Thread.currentThread;

public class TestSFTPRemoteConnector extends RemoteConnectorTestBase {

  @Override
  protected void setupServer() throws Exception {
    setupSSHD(path);
  }

  @Override
  protected SFTPRemoteConnectorForTest getConnector(RemoteConfigBean bean) {
    return new SFTPRemoteConnectorForTest(bean);
  }

  @Override
  protected String getScheme() {
    return "sftp";
  }

  @Test
  public void testPrivateKey() throws Exception {
    File privateKeyFile =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/id_rsa_test").getPath());

    SFTPRemoteConnectorForTest connector = new SFTPRemoteConnectorForTest(getBean(
        "sftp://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        null,
        privateKeyFile.toString(),
        "streamsets",
        null,
        true,
        null
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);

    connector.close();
  }

  @Test
  public void testPrivateKeyPlainText() throws Exception {
    File privateKeyFile =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/id_rsa_test").getPath());
    String privateKeyPlainText = Files.toString(privateKeyFile, Charset.forName("UTF-8"));

    SFTPRemoteConnectorForTest connector = new SFTPRemoteConnectorForTest(getBean(
        "sftp://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        null,
        null,
        "streamsets",
        null,
        true,
        privateKeyPlainText
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);

    connector.close();
  }

  @Test
  public void testPrivateKeyNoPassphrase() throws Exception {
    File privateKeyFile =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/id_rsa_test_unencrypted").getPath());

    SFTPRemoteConnectorForTest connector = new SFTPRemoteConnectorForTest(getBean(
        "sftp://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        null,
        privateKeyFile.toString(),
        "streamsets",
        null,
        true,
        null
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);

    connector.close();
  }

  @Test
  public void testPrivateKeyPlainTextNoPassphrase() throws Exception {
    File privateKeyFile =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/id_rsa_test_unencrypted").getPath());
    String privateKeyPlainText = Files.toString(privateKeyFile, Charset.forName("UTF-8"));

    SFTPRemoteConnectorForTest connector = new SFTPRemoteConnectorForTest(getBean(
        "sftp://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        null,
        null,
        "streamsets",
        null,
        true,
        privateKeyPlainText
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);

    connector.close();
  }

  @Test
  public void testPrivateKeyWrongPassphrase() throws Exception {
    File privateKeyFile =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/id_rsa_test").getPath());

    SFTPRemoteConnectorForTest connector = new SFTPRemoteConnectorForTest(getBean(
        "sftp://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        null,
        privateKeyFile.toString(),
        "wrongphrase",
        null,
        true,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.privateKey",
        Errors.REMOTE_10,
        "exception using cipher - please check password and data.",
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testPrivateKeyFileNotExist() throws Exception {
    File privateKeyFile = testFolder.newFile("id_rsa");
    privateKeyFile.delete();
    Assert.assertFalse(privateKeyFile.exists());

    SFTPRemoteConnectorForTest connector = new SFTPRemoteConnectorForTest(getBean(
        "sftp://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        null,
        privateKeyFile.toString(),
        "streamsets",
        null,
        true,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.privateKey",
        Errors.REMOTE_05,
        privateKeyFile.getAbsolutePath()
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testPrivateKeyPlainTextWrongPassphrase() throws Exception {
    File privateKeyFile =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/id_rsa_test").getPath());
    String privateKeyPlainText = Files.toString(privateKeyFile, Charset.forName("UTF-8"));

    SFTPRemoteConnectorForTest connector = new SFTPRemoteConnectorForTest(getBean(
        "sftp://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        null,
        null,
        "wrongphrase",
        null,
        true,
        privateKeyPlainText
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.privateKeyPlainText",
        Errors.REMOTE_10,
        "Decryption failed",
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testPrivateKeyInvalid() throws Exception {
    File privateKeyFile = testFolder.newFile("randomkey_rsa");
    Files.write("blah blah".getBytes(Charset.forName("UTF-8")), privateKeyFile);
    privateKeyFile.deleteOnExit();

    SFTPRemoteConnectorForTest connector = new SFTPRemoteConnectorForTest(getBean(
        "sftp://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        null,
        privateKeyFile.toString(),
        "streamsets",
        null,
        true,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.privateKey",
        Errors.REMOTE_10,
        "No provider available for Unknown key file",
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testPrivateKeyPlainTextInvalid() throws Exception {
    String privateKeyPlainText = "blah blah";

    SFTPRemoteConnectorForTest connector = new SFTPRemoteConnectorForTest(getBean(
        "sftp://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        null,
        null,
        "streamsets",
        null,
        true,
        privateKeyPlainText
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.privateKeyPlainText",
        Errors.REMOTE_10,
        "No provider available for Unknown key file",
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testStrictHostChecking() throws Exception {
    File knownHostsFile = testFolder.newFile("hosts");
    Files.write(getHostsFileEntry(), knownHostsFile);

    SFTPRemoteConnectorForTest connector = new SFTPRemoteConnectorForTest(getBean(
        "sftp://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        TESTPASS,
        null,
        null,
        knownHostsFile.toString(),
        false,
        null
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);

    connector.close();
  }

  @Test
  public void testStrictHostCheckingOtherHost() throws Exception {
    File knownHostsFile = testFolder.newFile("hosts");
    Files.write(getHostsFileEntry("other-host"), knownHostsFile);

    SFTPRemoteConnectorForTest connector = new SFTPRemoteConnectorForTest(getBean(
        "sftp://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        TESTPASS,
        null,
        null,
        knownHostsFile.toString(),
        false,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "SFTP/FTP/FTPS",
        "conf.remoteConfig.remoteAddress",
        Errors.REMOTE_11,
        "sftp://localhost:" + port + "/",
        "Could not verify `ssh-rsa` host key with fingerprint `" + getPublicKeyFingerprint() + "` for `localhost` on " +
            "port " + port,
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testStrictHostCheckingHostsFileNotExist() throws Exception {
    File knownHostsFile = testFolder.newFile("hosts");
    knownHostsFile.delete();
    Assert.assertFalse(knownHostsFile.exists());

    SFTPRemoteConnectorForTest connector = new SFTPRemoteConnectorForTest(getBean(
        "sftp://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        TESTPASS,
        null,
        null,
        knownHostsFile.toString(),
        false,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.knownHosts",
        Errors.REMOTE_03,
        knownHostsFile
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testStrictHostCheckingNoHostsFile() throws Exception {
    SFTPRemoteConnectorForTest connector = new SFTPRemoteConnectorForTest(getBean(
        "sftp://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        TESTPASS,
        null,
        null,
        null, // no hosts file
        false,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.strictHostChecking",
        Errors.REMOTE_04
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Override
  protected String getTestWrongPassMessage() {
    return "Exhausted available authentication methods";
  }

  @Override
  protected String getTestDirDoesntExistMessage(File dir) {
    return dir.getAbsolutePath() + " does not exist";
  }

  protected void testVerifyAndReconnectHelper(RemoteConnector connector) throws Exception {
    SFTPRemoteConnectorForTest con = (SFTPRemoteConnectorForTest) connector;
    // We'll fail the first call to sftpClient.ls, which will cause verifyAndReconnect to replace sshClient with
    // a new one
    con.sftpClient = Mockito.spy(con.sftpClient);
    SSHClient originalSshClient = con.sshClient;
    Mockito.when(con.sftpClient.ls()).thenThrow(new IOException("")).thenCallRealMethod();
    connector.verifyAndReconnect();
    Assert.assertNotEquals(originalSshClient, con.sshClient);
    verifyConnection(connector);
  }

  protected void stopServer() throws Exception {
    sshd.stop();
  }

  private class SFTPRemoteConnectorForTest extends SFTPRemoteConnector {
    protected SFTPRemoteConnectorForTest(RemoteConfigBean remoteConfig) {
      super(remoteConfig);
    }
  }

  @Override
  protected void verifyConnection(RemoteConnector connector) throws IOException {
    SFTPRemoteConnectorForTest con = (SFTPRemoteConnectorForTest) connector;
    verifyConnection(con, true);
  }

  private void verifyConnection(SFTPRemoteConnectorForTest connector, boolean verifyRebuilder) throws IOException {
    List<ChrootSFTPClient.SimplifiedRemoteResourceInfo> children = connector.sftpClient.ls();
    Assert.assertEquals(1, children.size());
    String line = IOUtils.toString(
        connector.sftpClient.openForReading(children.iterator().next().getPath()),
        Charset.forName("UTF-8")
    );
    Assert.assertEquals(TEXT, line);
    if (verifyRebuilder) {
      verifySshClientRebuilder(connector);
    }
  }

  private void verifySshClientRebuilder(SFTPRemoteConnectorForTest connector) throws IOException {
    // Verify that the SSHClientRebuilder can also create a working SSHClient
    SSHClient sshClient = connector.sshClientRebuilder.build();
    connector.sftpClient.setSFTPClient(sshClient.newSFTPClient());
    verifyConnection(connector, false);
  }
}
