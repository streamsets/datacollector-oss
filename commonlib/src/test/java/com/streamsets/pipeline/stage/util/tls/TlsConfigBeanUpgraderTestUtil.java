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
package com.streamsets.pipeline.stage.util.tls;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public abstract class TlsConfigBeanUpgraderTestUtil {

  public static void testHttpSslConfigBeanToTlsConfigBeanUpgrade(
      String confPrefix,
      StageUpgrader upgrader,
      int toVersion
  )
      throws StageException {
    List<Config> configs = new ArrayList<>();

    final String trustStoreFile = "/path/to/truststore";
    final String trustStorePassword = null;
    final String keyStoreFile = "/path/to/keystore";
    final String keyStorePassword = "keyStorePassword";

    final String oldTrustStoreFile = confPrefix + "sslConfig.trustStorePath";
    final String oldTrustStorePW = confPrefix + "sslConfig.trustStorePassword";
    final String oldKeyStoreFile = confPrefix + "sslConfig.keyStorePath";
    final String oldKeyStorePW = confPrefix + "sslConfig.keyStorePassword";

    configs.add(new Config(oldTrustStoreFile, trustStoreFile));
    configs.add(new Config(oldTrustStorePW, trustStorePassword));
    configs.add(new Config(oldKeyStoreFile, keyStoreFile));
    configs.add(new Config(oldKeyStorePW, keyStorePassword));

    UpgraderTestUtils.UpgradeMoveWatcher watcher = UpgraderTestUtils.snapshot(configs);

    upgrader.upgrade("", "", "", toVersion - 1, toVersion, configs);

    watcher.assertAllMoved(
        configs,
        oldTrustStoreFile, confPrefix + "tlsConfig.trustStoreFilePath",
        oldTrustStorePW, confPrefix + "tlsConfig.trustStorePassword",
        oldKeyStoreFile, confPrefix + "tlsConfig.keyStoreFilePath",
        oldKeyStorePW, confPrefix + "tlsConfig.keyStorePassword"
    );

    boolean tlsEnabledSeen = false;
    for (Config config : configs) {
      final String name = config.getName();
      if ((confPrefix+"tlsConfig.tlsEnabled").equals(name)) {
        tlsEnabledSeen = true;
      }
    }
    Assert.assertTrue("tlsEnabled property not seen in tlsConfig bean", tlsEnabledSeen);
    ensureNoHasStoreProperties(configs, confPrefix + "tlsConfig");
  }

  public static void testRawKeyStoreConfigsToTlsConfigBeanUpgrade(
      String configPrefix,
      StageUpgrader upgrader,
      int toVersion
  )
      throws StageException {
    testRawStoreConfigsToTlsConfigBeanUpgrade(configPrefix, upgrader, toVersion, "key");
  }

  public static void testRawTrustStoreConfigsToTlsConfigBeanUpgrade(
      String configPrefix,
      StageUpgrader upgrader,
      int toVersion
  )
      throws StageException {

    testRawStoreConfigsToTlsConfigBeanUpgrade(configPrefix, upgrader, toVersion, "trust");
  }

  private static void testRawStoreConfigsToTlsConfigBeanUpgrade(
      String configPrefix,
      StageUpgrader upgrader,
      int toVersion,
      String storeType
  )
      throws StageException {

    List<Config> configs = new ArrayList<>();
    final String oldKeyStoreFile = String.format("%s%sStoreFile", configPrefix, storeType);
    final String oldKeyStorePW = String.format("%s%sStorePassword", configPrefix, storeType);

    configs.add(new Config(oldKeyStoreFile, "/path/to/store"));
    configs.add(new Config(oldKeyStorePW, "storePassword"));

    UpgraderTestUtils.UpgradeMoveWatcher watcher = UpgraderTestUtils.snapshot(configs);

    upgrader.upgrade("", "", "", toVersion-1, toVersion, configs);

    watcher.assertAllMoved(
        configs,
        oldKeyStoreFile, String.format("%stlsConfigBean.%sStoreFilePath", configPrefix, storeType),
        oldKeyStorePW, String.format("%stlsConfigBean.%sStorePassword", configPrefix, storeType)
    );

    ensureNoHasStoreProperties(configs, configPrefix + "tlsConfigBean");
  }

  private static void ensureNoHasStoreProperties(List<Config> configs, String configPrefix) {
    String hasKeyStoreProperty = String.format("%s.hasKeyStore", configPrefix);
    String hasTrustStoreProperty = String.format("%s.hasTrustStore", configPrefix);
    UpgraderTestUtils.assertNoneExist(configs, hasKeyStoreProperty, hasTrustStoreProperty);
  }
}
