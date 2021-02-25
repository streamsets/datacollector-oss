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
package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.stage.connection.remote.Authentication;
import com.streamsets.pipeline.lib.remote.PrivateKeyProvider;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestRemoteDownloadSourceUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/RemoteDownloadDSource.yaml");
    upgrader = new SelectorStageUpgrader("stage", new RemoteDownloadSourceUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testUpgradeV1ToV2() throws Exception {
    // This should be removed.
    configs.add(new Config("conf.pollInterval", 1000));

    configs = new RemoteDownloadSourceUpgrader()
        .upgrade("a", "b", "v", 1, 2, configs);
    Assert.assertTrue(configs.isEmpty());
  }

  @Test
  public void testUpgradeV3ToV4() throws StageException {
    configs.add(new Config("conf.remoteAddress", "sftp://localhost:1234"));
    configs.add(new Config("conf.auth", Authentication.PASSWORD.name()));
    configs.add(new Config("conf.username", "user"));
    configs.add(new Config("conf.password", "pass"));
    configs.add(new Config("conf.privateKeyProvider", PrivateKeyProvider.FILE.name()));
    configs.add(new Config("conf.privateKey", "/my/private/key.txt"));
    configs.add(new Config("conf.privateKeyPlainText", "MY_PRIVATE_KEY"));
    configs.add(new Config("conf.privateKeyPassphrase", "secret"));
    configs.add(new Config("conf.userDirIsRoot", "true"));
    configs.add(new Config("conf.strictHostChecking", "true"));
    configs.add(new Config("conf.knownHosts", "/hosts"));
    configs.add(new Config("conf.leaveMeAlone", "foo"));

    UpgraderTestUtils.UpgradeMoveWatcher watcher = UpgraderTestUtils.snapshot(configs);

    new RemoteDownloadSourceUpgrader()
        .upgrade("a", "b", "v", 3, 4, configs);

    Assert.assertEquals(12, configs.size());

    watcher.assertAllMoved(
        configs,
        "conf.remoteAddress",
        "conf.remoteConfig.remoteAddress",
        "conf.auth",
        "conf.remoteConfig.auth",
        "conf.username",
        "conf.remoteConfig.username",
        "conf.password",
        "conf.remoteConfig.password",
        "conf.privateKeyProvider",
        "conf.remoteConfig.privateKeyProvider",
        "conf.privateKey",
        "conf.remoteConfig.privateKey",
        "conf.privateKeyPlainText",
        "conf.remoteConfig.privateKeyPlainText",
        "conf.privateKeyPassphrase",
        "conf.remoteConfig.privateKeyPassphrase",
        "conf.userDirIsRoot",
        "conf.remoteConfig.userDirIsRoot",
        "conf.strictHostChecking",
        "conf.remoteConfig.strictHostChecking",
        "conf.knownHosts",
        "conf.remoteConfig.knownHosts"
    );

    UpgraderTestUtils.assertExists(configs, "conf.leaveMeAlone", "foo");
  }

  @Test
  public void testV4ToV5() {
    Mockito.doReturn(4).when(context).getFromVersion();
    Mockito.doReturn(5).when(context).getToVersion();

    String dataFormatPrefix = "conf.dataFormatConfig.";
    configs.add(new Config(dataFormatPrefix + "preserveRootElement", true));
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, dataFormatPrefix + "preserveRootElement", false);
  }

  @Test
  public void testV5ToV6() {
    Mockito.doReturn(5).when(context).getFromVersion();
    Mockito.doReturn(6).when(context).getToVersion();

    String remoteConfigPrefix = "conf.remoteConfig.";
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, remoteConfigPrefix + "useRemoteKeyStore", false);
    UpgraderTestUtils.assertExists(configs, remoteConfigPrefix + "ftpsPrivateKey", "");
    UpgraderTestUtils.assertExists(configs, remoteConfigPrefix + "ftpsCertificateChain", new ArrayList<>());
    UpgraderTestUtils.assertExists(configs, remoteConfigPrefix + "ftpsTrustedCertificates", new ArrayList<>());
  }

  @Test
  public void testV6toV7() {
    Mockito.doReturn(6).when(context).getFromVersion();
    Mockito.doReturn(7).when(context).getToVersion();

    String remoteConfigPrefix = "conf.remoteConfig.";
    String connectionPrefix = remoteConfigPrefix + "connection.";
    String credentialsPrefix = connectionPrefix + "credentials.";

    configs.add(new Config(remoteConfigPrefix + "remoteAddress", "ftp://host:port"));
    configs.add(new Config(remoteConfigPrefix + "ftpsMode", "EXPLICIT"));
    configs.add(new Config(remoteConfigPrefix + "ftpsDataChannelProtectionLevel", "PRIVATE"));
    configs.add(new Config(remoteConfigPrefix + "auth", "NONE"));
    configs.add(new Config(remoteConfigPrefix + "username", ""));
    configs.add(new Config(remoteConfigPrefix + "password", ""));
    configs.add(new Config(remoteConfigPrefix + "strictHostChecking", "true"));
    configs.add(new Config(remoteConfigPrefix + "knownHosts", ""));
    configs.add(new Config(remoteConfigPrefix + "useFTPSClientCert", "false"));
    configs.add(new Config(remoteConfigPrefix + "ftpsTrustStoreProvider", "ALLOW_ALL"));
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, connectionPrefix + "protocol", "FTP");
    UpgraderTestUtils.assertExists(configs, connectionPrefix + "remoteAddress", "ftp://host:port");
    UpgraderTestUtils.assertExists(configs, connectionPrefix + "ftpsMode", "EXPLICIT");
    UpgraderTestUtils.assertExists(configs, connectionPrefix + "ftpsDataChannelProtectionLevel", "PRIVATE");
    UpgraderTestUtils.assertExists(configs, credentialsPrefix + "auth", "NONE");
    UpgraderTestUtils.assertExists(configs, credentialsPrefix + "username", "");
    UpgraderTestUtils.assertExists(configs, credentialsPrefix + "password", "");
    UpgraderTestUtils.assertExists(configs, credentialsPrefix + "strictHostChecking", "true");
    UpgraderTestUtils.assertExists(configs, credentialsPrefix + "knownHosts", "");
    UpgraderTestUtils.assertExists(configs, credentialsPrefix + "useFTPSClientCert", "false");
    UpgraderTestUtils.assertExists(configs, credentialsPrefix + "ftpsTrustStoreProvider", "ALLOW_ALL");

    configs.clear();
    configs.add(new Config(remoteConfigPrefix + "remoteAddress", "ftps://host:port"));
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, connectionPrefix + "protocol", "FTPS");

    configs.clear();
    configs.add(new Config(remoteConfigPrefix + "remoteAddress", "sftp://host:port"));
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, connectionPrefix + "protocol", "SFTP");
  }

}
