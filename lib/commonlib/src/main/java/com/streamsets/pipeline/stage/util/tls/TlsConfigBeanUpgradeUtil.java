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

import com.google.common.base.Strings;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.config.upgrade.UpgraderUtils;

import java.util.List;

public class TlsConfigBeanUpgradeUtil {
  public static void upgradeHttpSslConfigBeanToTlsConfigBean(List<Config> configs, String configPrefix) {
    final String newTrustStorePath = configPrefix + "tlsConfig.trustStoreFilePath";
    final String newKeyStorePath = configPrefix + "tlsConfig.keyStoreFilePath";

    UpgraderUtils.moveAllTo(
        configs,
        configPrefix + "sslConfig.trustStorePath", newTrustStorePath,
        configPrefix + "sslConfig.trustStorePassword", configPrefix + "tlsConfig.trustStorePassword",
        configPrefix + "sslConfig.keyStorePath", newKeyStorePath,
        configPrefix + "sslConfig.keyStorePassword", configPrefix + "tlsConfig.keyStorePassword"
    );

    boolean hasKeyStore = false;
    boolean hasTrustStore = false;

    for (Config config : configs) {
      if (newKeyStorePath.equals(config.getName())) {
        hasKeyStore = !Strings.isNullOrEmpty((String) config.getValue());
      } else if (newTrustStorePath.equals(config.getName())) {
        hasTrustStore = !Strings.isNullOrEmpty((String) config.getValue());
      }
    }

    configs.add(new Config(configPrefix + "tlsConfig.tlsEnabled", hasTrustStore || hasKeyStore));
  }

  public static void upgradeRawKeyStoreConfigsToTlsConfigBean(
      List<Config> configs,
      String configPrefix,
      String keyStorePathProperty,
      String keyStorePasswordProperty,
      String oldSslEnabledProperty,
      String newTlsEnabledProperty
  ) {
    upgradeRawStoreConfigsToTlsConfigBean(
        configs,
        configPrefix,
        keyStorePathProperty,
        keyStorePasswordProperty,
        oldSslEnabledProperty,
        newTlsEnabledProperty,
        "key"
    );
  }

  public static void upgradeRawTrustStoreConfigsToTlsConfigBean(
      List<Config> configs,
      String configPrefix,
      String trustStorePathProperty,
      String trustStorePasswordProperty,
      String oldSslEnabledProperty,
      String newTlsEnabledProperty
  ) {
    upgradeRawStoreConfigsToTlsConfigBean(
        configs,
        configPrefix,
        trustStorePathProperty,
        trustStorePasswordProperty,
        oldSslEnabledProperty,
        newTlsEnabledProperty,
        "trust"
    );
  }

  private static void upgradeRawStoreConfigsToTlsConfigBean(
      List<Config> configs,
      String configPrefix,
      String storePathProperty,
      String storePasswordProperty,
      String oldSslEnabledProperty,
      String newTlsEnabledProperty,
      String storeType
  ) {
    final String newStorePath = String.format("%stlsConfigBean.%sStoreFilePath", configPrefix, storeType);
    final String newTlsEnabled = String.format("%stlsConfigBean.%s", configPrefix, newTlsEnabledProperty);
    UpgraderUtils.moveAllTo(
        configs,
        configPrefix + oldSslEnabledProperty, newTlsEnabled,
        configPrefix + storePathProperty, newStorePath,
        configPrefix + storePasswordProperty, String.format(
            "%stlsConfigBean.%sStorePassword",
            configPrefix,
            storeType
        )
    );
  }
}
