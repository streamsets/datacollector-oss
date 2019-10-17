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
package com.streamsets.pipeline.stage.processor.http;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;
import com.streamsets.pipeline.lib.http.HttpCompressionType;
import com.streamsets.pipeline.lib.http.JerseyClientUtil;
import com.streamsets.pipeline.lib.http.logging.HttpConfigUpgraderUtil;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.util.tls.TlsConfigBeanUpgradeUtil;

import java.util.ArrayList;
import java.util.List;

/** {@inheritDoc} */
public class HttpProcessorUpgrader implements StageUpgrader {
  private static final String CONF = "conf";
  private static final String CLIENT = "client";

  private final List<Config> configsToRemove = new ArrayList<>();
  private final List<Config> configsToAdd = new ArrayList<>();

  private static final Joiner joiner = Joiner.on(".");

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 1:
        JerseyClientUtil.upgradeToJerseyConfigBean(configs);
        if (toVersion == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        if (toVersion == 3) {
          break;
        }
        // fall through
      case 3:
        upgradeV3ToV4(configs);
        if (toVersion == 4) {
          break;
        }
        // fall through
      case 4:
        upgradeV4ToV5(configs);
        if (toVersion == 5) {
          break;
        }
        // fall through
      case 5:
        upgradeV5ToV6(configs);
        if (toVersion == 6) {
          break;
        }
        // fall through
      case 6:
        upgradeV6ToV7(configs);
        if (toVersion == 7) {
          break;
        }
        // fall through
      case 7:
        upgradeV7ToV8(configs);
        if (toVersion == 8) {
          break;
        }
        // fall through
      case 8:
        upgradeV8ToV9(configs);
        if (toVersion == 9) {
          break;
        }
        // fall through
      case 9:
        upgradeV9ToV10(configs);
        if (toVersion == 10) {
          break;
        }
        // fall through
      case 10:
        upgradeV10ToV11(configs);
        if (toVersion == 11) {
          break;
        }
      // fall through
      case 11:
        upgradeV11ToV12(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, "headerOutputLocation"), "HEADER"));
    // Default for upgrades is different so that we don't accidentally clobber possibly pre-existing attributes.
    configs.add(new Config(joiner.join(CONF, "headerAttributePrefix"), "http-"));
    configs.add(new Config(joiner.join(CONF, "headerOutputField"), ""));
  }

  private void upgradeV3ToV4(List<Config> configs) {
    configsToAdd.clear();
    configsToRemove.clear();

    for (Config config : configs) {
      if (joiner.join(CONF, CLIENT, "requestTimeoutMillis").equals(config.getName())) {
        configsToRemove.add(config);
        configsToAdd.add(new Config(joiner.join(CONF, CLIENT, "readTimeoutMillis"), config.getValue()));
      }
    }

    configsToAdd.add(new Config(joiner.join(CONF, CLIENT, "connectTimeoutMillis"), "0"));
    configsToAdd.add(new Config(joiner.join(CONF, "maxRequestCompletionSecs"), "60"));

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }

  private void upgradeV4ToV5(List<Config> configs) {
    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);
  }

  private static void upgradeV5ToV6(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, CLIENT, "useOAuth2"), false));
  }

  private void upgradeV6ToV7(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, "rateLimit"), 0));
  }

  private void upgradeV7ToV8(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, CLIENT, "httpCompression"), HttpCompressionType.NONE));
  }

  private void upgradeV8ToV9(List<Config> configs) {
    TlsConfigBeanUpgradeUtil.upgradeHttpSslConfigBeanToTlsConfigBean(configs, "conf.client.");
  }

  private void upgradeV9ToV10(List<Config> configs) {
    configsToAdd.clear();
    configsToRemove.clear();

    String key = joiner.join(CONF, "rateLimit");

    for (Config config : configs) {
      if (key.equals(config.getName())) {
        configsToRemove.add(config);
        configsToAdd.add(new Config(key, (int)config.getValue() == 0 ? 0 : (int)(1000.0 / (int)config.getValue())));
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }

  private void upgradeV10ToV11(List<Config> configs) {
    HttpConfigUpgraderUtil.addDefaultRequestLoggingConfigs(configs, "conf.client");
  }

  private void upgradeV11ToV12(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, "multipleValuesBehavior"), MultipleValuesBehavior.FIRST_ONLY));
  }
}
