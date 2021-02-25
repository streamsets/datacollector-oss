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
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import com.streamsets.pipeline.stage.connection.remote.Authentication;
import com.streamsets.pipeline.stage.connection.remote.RemoteConnection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public abstract class RemoteConnectorTestBase extends FTPAndSSHDUnitTest {

  protected static final String TEXT = "hello";

  @BeforeClass
  public static void setUpClass() {
    DataCollectorServicesUtils.loadDefaultServices();
  }

  @Before
  public void setUp() throws Exception {
    path = testFolder.newFolder("home").getAbsolutePath();
    File file = new File(path, "file.txt");
    Files.write(TEXT.getBytes(Charset.forName("UTF-8")), file);
    setupServer();
  }

  protected abstract void setupServer() throws Exception;

  protected abstract RemoteConnector getConnector(RemoteConfigBean bean);

  protected abstract String getScheme();

  protected abstract void verifyConnection(RemoteConnector connector) throws IOException;

  @Test
  public void testNoError() throws Exception {
    RemoteConnector connector = getConnector(getBean(
        getScheme() + "://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        TESTPASS,
        null,
        null,
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
  public void testWrongPass() throws Exception {
    RemoteConnector connector = getConnector(getBean(
        getScheme() + "://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        "wrongpass",
        null,
        null,
        null,
        true,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "SFTP/FTP/FTPS",
        "conf.remoteConfig.remoteAddress",
        Errors.REMOTE_11,
        getScheme() + "://localhost:" + port + "/",
        getTestWrongPassMessage(),
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  protected abstract String getTestWrongPassMessage();

  @Test
  public void testUserDirIsNotRoot() throws Exception {
    RemoteConnector connector = getConnector(getBean(
        getScheme() + "://localhost:" + port + "/" + path,
        false,
        false,
        TESTUSER,
        TESTPASS,
        null,
        null,
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
  public void testDirDoesntExist() throws Exception {
    File dir = new File(path, "dir");
    Assert.assertFalse(dir.exists());

    RemoteConnector connector = getConnector(getBean(
        getScheme() + "://localhost:" + port + "/" + dir.getName(),
        true,
        false,
        TESTUSER,
        TESTPASS,
        null,
        null,
        null,
        true,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "SFTP/FTP/FTPS",
        "conf.remoteConfig.remoteAddress",
        Errors.REMOTE_11,
        getScheme() + "://localhost:" + port + "/dir",
        getTestDirDoesntExistMessage(dir),
        null
    );
    Assert.assertEquals(1, issues.size());
    Assert.assertFalse(dir.exists());

    connector.close();
  }

  protected abstract String getTestDirDoesntExistMessage(File dir);

  @Test
  public void testDirDoesntExistAndMake() throws Exception {
    File dir = new File(path, "dir");
    Assert.assertFalse(dir.exists());

    RemoteConnector connector = getConnector(getBean(
        getScheme() + "://localhost:" + port + "/" + dir.getName(),
        true,
        true,
        TESTUSER,
        TESTPASS,
        null,
        null,
        null,
        true,
        null
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    Assert.assertTrue(dir.exists());

    connector.close();
  }

  @Test
  public void testVerifyAndReconnect() throws Exception {
    RemoteConnector connector = getConnector(getBean(
        getScheme() + "://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        TESTPASS,
        null,
        null,
        null,
        true,
        null
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);

    testVerifyAndReconnectHelper(connector);

    connector.close();
  }

  protected abstract void testVerifyAndReconnectHelper(RemoteConnector connector) throws Exception;

  @Test
  public void testVerifyAndReconnectFailRetries() throws Exception {
    RemoteConnector connector = getConnector(getBean(
        getScheme() + "://localhost:" + port + "/",
        true,
        false,
        TESTUSER,
        TESTPASS,
        null,
        null,
        null,
        true,
        null
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);

    // We'lll stop the server so verifyAndReconnect will exhaust all retries
    stopServer();
    try {
      connector.verifyAndReconnect();
      Assert.fail("Expected StageException");
    } catch (StageException e) {
      Assert.assertEquals(Errors.REMOTE_09, e.getErrorCode());
    } finally {
      connector.close();
    }
  }

  protected abstract void stopServer() throws Exception;

  protected RemoteConfigBean getBean(
      String remoteHost,
      boolean userDirIsRoot,
      boolean createPathIfNotExists,
      String username,
      String password,
      String privateKey,
      String passphrase,
      String knownHostsFile,
      boolean noHostChecking,
      String privateKeyPlainText
  ) {
    RemoteConfigBean configBean = new RemoteConfigBean();
    configBean.connection = new RemoteConnection();
    configBean.connection.remoteAddress = remoteHost;
    configBean.userDirIsRoot = userDirIsRoot;
    configBean.createPathIfNotExists = createPathIfNotExists;
    configBean.connection.credentials.username = () -> username;
    configBean.connection.credentials.password = () -> password;
    configBean.privateKey = privateKey;
    configBean.privateKeyPlainText = () -> privateKeyPlainText;
    configBean.privateKeyPassphrase = () -> passphrase;
    configBean.connection.credentials.knownHosts = knownHostsFile;
    configBean.connection.credentials.strictHostChecking = !noHostChecking;
    if (password != null) {
      configBean.connection.credentials.auth = Authentication.PASSWORD;
    } else {
      configBean.connection.credentials.auth = Authentication.PRIVATE_KEY;
    }
    if (privateKeyPlainText == null) {
      configBean.privateKeyProvider = PrivateKeyProvider.FILE;
    } else {
      configBean.privateKeyProvider = PrivateKeyProvider.PLAIN_TEXT;
    }
    return configBean;
  }

  protected List<Stage.ConfigIssue> initWithNoIssues(
      RemoteConnector connector
  ) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    Label remoteGroup = Mockito.mock(Label.class);
    Mockito.when(remoteGroup.getLabel()).thenReturn("SFTP/FTP/FTPS");
    Label credGroup = Mockito.mock(Label.class);
    Mockito.when(credGroup.getLabel()).thenReturn("CREDENTIALS");
    connector.initAndConnect(
        issues,
        Mockito.mock(Stage.Context.class),
        URI.create(connector.remoteConfig.connection.remoteAddress),
        remoteGroup,
        credGroup
    );
    return issues;
  }

  protected List<Stage.ConfigIssue> initAndCheckIssue(
      RemoteConnector connector,
      String expectedConfigGroup,
      String expectedConfigName,
      ErrorCode expectedErrorCode,
      Object... expectedArgs
  ) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    Label remoteGroup = Mockito.mock(Label.class);
    Mockito.when(remoteGroup.getLabel()).thenReturn("SFTP/FTP/FTPS");
    Label credGroup = Mockito.mock(Label.class);
    Mockito.when(credGroup.getLabel()).thenReturn("CREDENTIALS");
    connector.initAndConnect(
        issues,
        createContextAndCheckIssue(expectedConfigGroup, expectedConfigName, expectedErrorCode, expectedArgs),
        URI.create(connector.remoteConfig.connection.remoteAddress),
        remoteGroup,
        credGroup
    );
    return issues;
  }

  private Stage.Context createContextAndCheckIssue(
      String expectedConfigGroup,
      String expectedConfigName,
      ErrorCode expectedErrorCode,
      Object... expectedArgs
  ) {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any(), Matchers.<Object>anyVararg()))
        .thenAnswer(new Answer<ConfigIssue>() {
          @Override
          public ConfigIssue answer(InvocationOnMock invocation) throws Throwable {
            // +3 because getArguments() also includes the first 3 args not in expectedArgs
            // (i.e. expectedConfigGroup, expectedConfigName, and expectedErrorCode)
            Assert.assertEquals(expectedArgs.length + 3, invocation.getArguments().length);

            String configGroup = invocation.getArgumentAt(0, String.class);
            Assert.assertEquals(expectedConfigGroup, configGroup);

            String configName = invocation.getArgumentAt(1, String.class);
            Assert.assertEquals(expectedConfigName, configName);

            ErrorCode errorCode = invocation.getArgumentAt(2, ErrorCode.class);
            Assert.assertEquals(expectedErrorCode, errorCode);

            for (int i = 0; i < expectedArgs.length; i++) {
              // null indicates we should skip because it's something we can't easily verify (e.g. an Exception)
              if (expectedArgs[i] != null) {
                Assert.assertEquals(expectedArgs[i], invocation.getArgumentAt(i + 3, Object.class));
              }
            }

            ConfigIssue issue = Mockito.mock(ConfigIssue.class);
            return issue;
          }
        });
    return context;
  }
}
