/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.executor.remote;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestRemoteLocationExecutorUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/RemoteLocationDExecutor.yaml");
    upgrader = new SelectorStageUpgrader("stage", null, yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testUpgradeV1ToV2() {
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    String remoteConfigPrefix = "conf.remoteConfig.";
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, remoteConfigPrefix + "useRemoteKeyStore", false);
    UpgraderTestUtils.assertExists(configs, remoteConfigPrefix + "ftpsPrivateKey", "");
    UpgraderTestUtils.assertExists(configs, remoteConfigPrefix + "ftpsCertificateChain", new ArrayList<>());
    UpgraderTestUtils.assertExists(configs, remoteConfigPrefix + "ftpsTrustedCertificates", new ArrayList<>());
  }

  @Test
  public void testV2toV3() {
    Mockito.doReturn(2).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

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
