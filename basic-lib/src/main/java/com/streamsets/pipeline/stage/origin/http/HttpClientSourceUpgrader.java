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
package com.streamsets.pipeline.stage.origin.http;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.JerseyClientUtil;
import com.streamsets.pipeline.lib.http.logging.HttpConfigUpgraderUtil;
import com.streamsets.pipeline.stage.util.tls.TlsConfigBeanUpgradeUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpClientSourceUpgrader implements StageUpgrader {
  private static final String CONF = "conf";
  private static final String CLIENT = "client";
  private static final String BASIC = "basic";
  private static final String AUTH_TYPE = "authType";
  private static final String OAUTH = "oauth";
  private static final String DATA_FORMAT_CONFIG = "dataFormatConfig";
  private static final String PAGINATION_CONFIG = "pagination";

  private static final Joiner joiner = Joiner.on(".");

  private final List<Config> configsToRemove = new ArrayList<>();
  private final List<Config> configsToAdd = new ArrayList<>();

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
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
        JerseyClientUtil.upgradeToJerseyConfigBean(configs);
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
        if (toVersion == 12) {
          break;
        }
        // fall through
      case 12:
        upgradeV12ToV13(configs);
        if (toVersion == 13) {
          break;
        }
        // fall through
      case 13:
        upgradeV13ToV14(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV11ToV12(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, PAGINATION_CONFIG, "keepAllFields"), false));
  }

  private void upgradeV12ToV13(List<Config> configs) {
    TlsConfigBeanUpgradeUtil.upgradeHttpSslConfigBeanToTlsConfigBean(configs, "conf.client.");
  }

  private void upgradeV13ToV14(List<Config> configs) {
    HttpConfigUpgraderUtil.addDefaultRequestLoggingConfigs(configs, "conf.client");
  }

  private static void upgradeV8ToV9(List<Config> configs) {
    DataFormatUpgradeHelper.ensureAvroSchemaExists(configs, joiner.join(CONF, DATA_FORMAT_CONFIG));
    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);
  }

  private static void upgradeV9ToV10(List<Config> configs) {
    ArrayList<Map<String, Object>> statusActions = new ArrayList<>();
    final Map<String, Object> defaultStatusAction = new HashMap<>();
    defaultStatusAction.put("statusCode", HttpStatusResponseActionConfigBean.DEFAULT_STATUS_CODE);
    defaultStatusAction.put("action", HttpStatusResponseActionConfigBean.DEFAULT_ACTION);
    defaultStatusAction.put("backoffInterval",
            HttpStatusResponseActionConfigBean.DEFAULT_BACKOFF_INTERVAL_MS);
    defaultStatusAction.put("maxNumRetries",
            HttpStatusResponseActionConfigBean.DEFAULT_MAX_NUM_RETRIES);
    statusActions.add(defaultStatusAction);
    configs.add(new Config("conf.responseStatusActionConfigs", statusActions));
    configs.add(new Config(joiner.join(CONF, "responseTimeoutActionConfig", "action"),
            HttpTimeoutResponseActionConfigBean.DEFAULT_ACTION));
    configs.add(new Config(joiner.join(CONF, "responseTimeoutActionConfig", "backoffInterval"),
            HttpResponseActionConfigBean.DEFAULT_BACKOFF_INTERVAL_MS));
    configs.add(new Config(joiner.join(CONF, "responseTimeoutActionConfig", "maxNumRetries"),
            HttpResponseActionConfigBean.DEFAULT_MAX_NUM_RETRIES));
  }

  private void upgradeV1ToV2(List<Config> configs) {
    configsToRemove.clear();
    configsToAdd.clear();

    for (Config config : configs) {
      switch (config.getName()) {
        case "dataFormat":
        case "resourceUrl":
        case "httpMethod":
        case "requestData":
        case "requestTimeoutMillis":
        case "httpMode":
        case "pollingInterval":
        case "entityDelimiter":
          configsToAdd.add(new Config(joiner.join(CONF, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        case "isOAuthEnabled":
          AuthenticationType authType = AuthenticationType.NONE;
          if ((boolean)config.getValue()) {
            authType = AuthenticationType.OAUTH;
          }
          configsToAdd.add(new Config(joiner.join(CONF, "authType"), authType));
          configsToRemove.add(config);
          break;
        case "jsonMode":
          configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, "jsonContent"), config.getValue()));
          configsToRemove.add(config);
          break;
        case "consumerKey":
        case "consumerSecret":
        case "token":
        case "tokenSecret":
          configsToAdd.add(new Config(joiner.join(CONF, OAUTH, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        case "batchSize":
          configsToAdd.add(new Config(joiner.join(CONF, BASIC, "maxBatchSize"), config.getValue()));
          configsToRemove.add(config);
          break;
        case "maxBatchWaitTime":
          configsToAdd.add(new Config(joiner.join(CONF, BASIC, "maxWaitTime"), config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          // no op
      }
    }
    configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, "csvSkipStartLines"), 0));

    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private static void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("conf.useProxy", false));
    configs.add(new Config("conf.proxy.uri", ""));
    configs.add(new Config("conf.proxy.username", ""));
    configs.add(new Config("conf.proxy.password", ""));
  }

  private void upgradeV3ToV4(List<Config> configs) {
    configsToRemove.clear();
    configsToAdd.clear();

    for (Config config : configs) {
      if (joiner.join(CONF, AUTH_TYPE)
          .equals(config.getName()) && AuthenticationType.BASIC.name() == config.getValue()) {
        configsToRemove.add(config);
        configsToAdd.add(new Config(joiner.join(CONF, AUTH_TYPE), AuthenticationType.UNIVERSAL));
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }

  private static void upgradeV4ToV5(List<Config> configs) {
    configs.add(new Config("conf.headers", new ArrayList<Map<String, String>>()));
  }

  private void upgradeV5ToV6(List<Config> configs) {
    configsToRemove.clear();
    configsToAdd.clear();

    for (Config config : configs) {
      if (joiner.join(CONF, "requestData").equals(config.getName())) {
        configsToRemove.add(config);
        configsToAdd.add(new Config(joiner.join(CONF, "requestBody"), config.getValue()));
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }

  private void upgradeV7ToV8(List<Config> configs) {
    configsToRemove.clear();
    configsToAdd.clear();

    for (Config config : configs) {
      if (joiner.join(CONF, "entityDelimiter").equals(config.getName())) {
        configsToRemove.add(config);
      }

      if (joiner.join(CONF, CLIENT, "requestTimeoutMillis").equals(config.getName())) {
        configsToRemove.add(config);
        configsToAdd.add(new Config(joiner.join(CONF, CLIENT, "readTimeoutMillis"), config.getValue()));
      }
    }

    configsToAdd.add(new Config(joiner.join(CONF, CLIENT, "connectTimeoutMillis"), 0));
    configsToAdd.add(new Config(joiner.join(CONF, PAGINATION_CONFIG, "mode"), PaginationMode.NONE));
    configsToAdd.add(new Config(joiner.join(CONF, PAGINATION_CONFIG, "startAt"), 0));
    configsToAdd.add(new Config(joiner.join(CONF, PAGINATION_CONFIG, "resultFieldPath"), ""));
    configsToAdd.add(new Config(joiner.join(CONF, PAGINATION_CONFIG, "rateLimit"), 2000));

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);

    // Pagination config bean should be added automatically as its initialized.
  }

  private static void upgradeV10ToV11(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, CLIENT,"useOAuth2"), false));
  }

}
