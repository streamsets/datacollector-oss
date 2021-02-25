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
import com.streamsets.pipeline.lib.tls.KeyStoreType;
import com.streamsets.pipeline.stage.connection.remote.FTPSDataChannelProtectionLevel;
import com.streamsets.pipeline.stage.connection.remote.FTPSMode;
import com.streamsets.pipeline.stage.connection.remote.FTPSTrustStore;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.Certificate;
import java.util.List;

public class TestFTPRemoteConnectorFTPS extends TestFTPRemoteConnector {

  @Override
  protected void setupServer() throws Exception {
    setupFTPSServer(path, KeyStoreType.JKS, null, false);
  }

  @Override
  protected String getScheme() {
    return "ftps";
  }

  @Test
  public void testImplicit() throws Exception {
    // We've been using EXPLICIT in all other tests; here we make sure IMPLICIT works
    super.after();
    setupFTPSServer(path, KeyStoreType.JKS, null, true);

    RemoteConnector connector = getConnector(getBean(
        FTPSMode.IMPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        null,
        null,
        null,
        FTPSTrustStore.ALLOW_ALL,
        null,
        null,
        null
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);

    connector.close();
  }

  @Test
  public void testProtectionLevelClear() throws Exception {
    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.CLEAR,
        null,
        null,
        null,
        FTPSTrustStore.ALLOW_ALL,
        null,
        null,
        null
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);
    Assert.assertFalse(
        ftpServer.getListener("default").getActiveSessions().iterator().next().getDataConnection().isSecure());

    connector.close();
  }

  @Test
  public void testProtectionLevelPrivate() throws Exception {
    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        null,
        null,
        null,
        FTPSTrustStore.ALLOW_ALL,
        null,
        null,
        null
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);
    Assert.assertTrue(
        ftpServer.getListener("default").getActiveSessions().iterator().next().getDataConnection().isSecure());

    connector.close();
  }

  @Test
  public void testClientCertEmptyFile() throws Exception {
    super.after();
    setupFTPSServer(path, KeyStoreType.JKS, KeyStoreType.JKS, false);

    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        "",
        KeyStoreType.JKS,
        KEYSTORE_PASSWORD,
        FTPSTrustStore.ALLOW_ALL,
        null,
        null,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.ftpsClientCertKeystoreFile",
        Errors.REMOTE_12
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testClientCertWrongPassword() throws Exception {
    super.after();
    setupFTPSServer(path, KeyStoreType.JKS, KeyStoreType.JKS, false);

    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        clientCertificateKeystore.getAbsolutePath(),
        KeyStoreType.JKS,
        KEYSTORE_PASSWORD + "wrong",
        FTPSTrustStore.ALLOW_ALL,
        null,
        null,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.ftpsClientCertKeystoreFile",
        Errors.REMOTE_14,
        "key",
        clientCertificateKeystore.getAbsolutePath(),
        "Keystore was tampered with, or password was incorrect",
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testClientCertFileNotExist() throws Exception {
    super.after();
    setupFTPSServer(path, KeyStoreType.JKS, KeyStoreType.JKS, false);

    String badFile = clientCertificateKeystore.getAbsolutePath() + "bad";
    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        badFile,
        KeyStoreType.JKS,
        KEYSTORE_PASSWORD,
        FTPSTrustStore.ALLOW_ALL,
        null,
        null,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.ftpsClientCertKeystoreFile",
        Errors.REMOTE_14,
        "key",
        badFile,
        badFile + " (No such file or directory)",
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testClientCertKeyManagerFail() throws Exception {
    super.after();
    setupFTPSServer(path, KeyStoreType.JKS, KeyStoreType.JKS, false);
    ruinClientKeystoreToBreakKeyManager();

    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        clientCertificateKeystore.getAbsolutePath(),
        KeyStoreType.JKS,
        KEYSTORE_PASSWORD,
        FTPSTrustStore.ALLOW_ALL,
        null,
        null,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.ftpsClientCertKeystoreFile",
        Errors.REMOTE_15,
        "key",
        "Cannot recover key",
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testClientCertJKS() throws Exception {
    super.after();
    setupFTPSServer(path, KeyStoreType.JKS, KeyStoreType.JKS, false);

    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        clientCertificateKeystore.getAbsolutePath(),
        KeyStoreType.JKS,
        KEYSTORE_PASSWORD,
        FTPSTrustStore.ALLOW_ALL,
        null,
        null,
        null
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);

    connector.close();
  }

  @Test
  public void testClientCertPKC12() throws Exception {
    super.after();
    setupFTPSServer(path, KeyStoreType.JKS, KeyStoreType.PKCS12, false);

    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        clientCertificateKeystore.getAbsolutePath(),
        KeyStoreType.PKCS12,
        KEYSTORE_PASSWORD,
        FTPSTrustStore.ALLOW_ALL,
        null,
        null,
        null
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);

    connector.close();
  }

  @Test
  public void testClientCertReject() throws Exception {
    super.after();
    setupFTPSServer(path, KeyStoreType.JKS, KeyStoreType.JKS, false);

    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        // Client cert not in Server's Truststore
        generateCertificateKeystore(KeyStoreType.JKS).getAbsolutePath(),
        KeyStoreType.JKS,
        KEYSTORE_PASSWORD,
        FTPSTrustStore.ALLOW_ALL,
        null,
        null,
        null
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "SFTP/FTP/FTPS",
        "conf.remoteConfig.remoteAddress",
        Errors.REMOTE_11,
        getScheme() + "://localhost:" + port + "/",
        "Could not connect to FTP server on \"localhost\".",
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testTrustStoreEmptyFile() throws Exception {
    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        null,
        null,
        null,
        FTPSTrustStore.FILE,
        "",
        KeyStoreType.JKS,
        KEYSTORE_PASSWORD
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.ftpsTruststoreFile",
        Errors.REMOTE_13
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testTrustStoreWrongPassword() throws Exception {
    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        null,
        null,
        null,
        FTPSTrustStore.FILE,
        serverCertificateKeystore.getAbsolutePath(),
        KeyStoreType.JKS,
        KEYSTORE_PASSWORD + "wrong"
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.ftpsTruststoreFile",
        Errors.REMOTE_14,
        "trust",
        serverCertificateKeystore.getAbsolutePath(),
        "Keystore was tampered with, or password was incorrect",
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testTrustStoreFileNotExist() throws Exception {
    String badFile = serverCertificateKeystore.getAbsolutePath() + "bad";

    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        null,
        null,
        null,
        FTPSTrustStore.FILE,
        badFile,
        KeyStoreType.JKS,
        KEYSTORE_PASSWORD
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "CREDENTIALS",
        "conf.remoteConfig.ftpsTruststoreFile",
        Errors.REMOTE_14,
        "trust",
        badFile,
        badFile + " (No such file or directory)",
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testTrustStoreTrustManagerFail() throws Exception {
    String originalValue = Security.getProperty("ssl.TrustManagerFactory.algorithm");
    try {
      // This'll cause the TrustManager to fail
      Security.setProperty("ssl.TrustManagerFactory.algorithm", "not-exist");

      RemoteConnector connector = getConnector(getBean(
          FTPSMode.EXPLICIT,
          FTPSDataChannelProtectionLevel.PRIVATE,
          null,
          null,
          null,
          FTPSTrustStore.FILE,
          serverCertificateKeystore.getAbsolutePath(),
          KeyStoreType.JKS,
          KEYSTORE_PASSWORD
      ));

      List<Stage.ConfigIssue> issues = initAndCheckIssue(
          connector,
          "CREDENTIALS",
          "conf.remoteConfig.ftpsTruststoreFile",
          Errors.REMOTE_15,
          "trust",
          "not-exist TrustManagerFactory not available",
          null
      );
      Assert.assertEquals(1, issues.size());

      connector.close();
    } finally {
      if (originalValue == null) {
        System.clearProperty("ssl.TrustManagerFactory.algorithm");
      } else {
        Security.setProperty("ssl.TrustManagerFactory.algorithm", originalValue);
      }
    }
  }

  @Test
  public void testTrustStoreJVM() throws Exception {
    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        null,
        null,
        null,
        FTPSTrustStore.JVM_DEFAULT,
        null,
        null,
        null
    ));

    // We can't connect successfully because the JVM truststore doesn't trust the FTP Server
    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "SFTP/FTP/FTPS",
        "conf.remoteConfig.remoteAddress",
        Errors.REMOTE_11,
        getScheme() + "://localhost:" + port + "/",
        "Could not connect to FTP server on \"localhost\".",
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  @Test
  public void testTrustStoreFileJKS() throws Exception {
    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        null,
        null,
        null,
        FTPSTrustStore.FILE,
        serverCertificateKeystore.getAbsolutePath(),
        KeyStoreType.JKS,
        KEYSTORE_PASSWORD
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);

    connector.close();
  }

  @Test
  public void testTrustStoreFilePKC12() throws Exception {
    super.after();
    setupFTPSServer(path, KeyStoreType.PKCS12, null, false);

    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        null,
        null,
        null,
        FTPSTrustStore.FILE,
        serverCertificateKeystore.getAbsolutePath(),
        KeyStoreType.PKCS12,
        KEYSTORE_PASSWORD
    ));

    List<Stage.ConfigIssue> issues = initWithNoIssues(connector);
    Assert.assertEquals(0, issues.size());
    verifyConnection(connector);

    connector.close();
  }

  @Test
  public void testTrustStoreFileReject() throws Exception {
    RemoteConnector connector = getConnector(getBean(
        FTPSMode.EXPLICIT,
        FTPSDataChannelProtectionLevel.PRIVATE,
        null,
        null,
        null,
        FTPSTrustStore.FILE,
        // Server cert not in Client's Truststore
        generateCertificateKeystore(KeyStoreType.JKS).getAbsolutePath(),
        KeyStoreType.JKS,
        KEYSTORE_PASSWORD
    ));

    List<Stage.ConfigIssue> issues = initAndCheckIssue(
        connector,
        "SFTP/FTP/FTPS",
        "conf.remoteConfig.remoteAddress",
        Errors.REMOTE_11,
        getScheme() + "://localhost:" + port + "/",
        "Could not connect to FTP server on \"localhost\".",
        null
    );
    Assert.assertEquals(1, issues.size());

    connector.close();
  }

  protected RemoteConfigBean getBean(
      FTPSMode mode,
      FTPSDataChannelProtectionLevel protLevel,
      String keystoreFile,
      KeyStoreType keystoreType,
      String keystorePassword,
      FTPSTrustStore truststoreProvider,
      String truststoreFile,
      KeyStoreType truststoreType,
      String truststorePassword
  ) {
    RemoteConfigBean configBean = getBean(
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
    );
    configBean.connection.ftpsMode = mode;
    configBean.connection.ftpsDataChannelProtectionLevel = protLevel;
    if (keystoreFile != null) {
      configBean.connection.credentials.useFTPSClientCert = true;
      configBean.ftpsClientCertKeystoreFile = keystoreFile;
      configBean.ftpsClientCertKeystoreType = keystoreType;
      configBean.ftpsClientCertKeystorePassword = () -> keystorePassword;
    } else {
      configBean.connection.credentials.useFTPSClientCert = false;
    }
    configBean.connection.credentials.ftpsTrustStoreProvider = truststoreProvider;
    if (truststoreProvider == FTPSTrustStore.FILE) {
      configBean.ftpsTruststoreFile = truststoreFile;
      configBean.ftpsTruststoreType = truststoreType;
      configBean.ftpsTruststorePassword = () -> truststorePassword;
    }
    return configBean;
  }

  private void ruinClientKeystoreToBreakKeyManager() throws Exception {
    KeyStore keyStore = KeyStore.getInstance(KeyStoreType.JKS.getJavaValue());
    try (InputStream fis = new FileInputStream(clientCertificateKeystore)) {
      keyStore.load(fis, KEYSTORE_PASSWORD.toCharArray());
    }
    // This changes the password for the first entry to be something else so the KeyManager will try the wrong password
    // Though we keep the keystore password itself intact
    String alias = keyStore.aliases().nextElement();
    keyStore.setKeyEntry(
        alias,
        keyStore.getKey(alias, KEYSTORE_PASSWORD.toCharArray()),
        "something else".toCharArray(),
        new Certificate[]{keyStore.getCertificate(alias)}
    );
    try (FileOutputStream fos = new FileOutputStream(clientCertificateKeystore)) {
      keyStore.store(fos, KEYSTORE_PASSWORD.toCharArray());
    }
  }
}
