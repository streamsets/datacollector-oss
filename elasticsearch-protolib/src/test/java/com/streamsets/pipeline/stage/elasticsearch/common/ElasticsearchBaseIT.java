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
package com.streamsets.pipeline.stage.elasticsearch.common;

import com.streamsets.testing.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ElasticsearchBaseIT {
  private static final String esName = UUID.randomUUID().toString();

  protected static Node esServer;
  protected static int esHttpPort;

  @ClassRule
  public static TemporaryFolder esDir = new TemporaryFolder();

  public static void setUp() throws Exception {
    esHttpPort = NetworkUtils.getRandomPort();
    Map<String, Object> configs = new HashMap<>();
    configs.put("cluster.name", esName);
    configs.put("http.enabled", true);
    configs.put("http.port", esHttpPort);
    configs.put("path.home", esDir.getRoot().getAbsolutePath());
    configs.put("path.conf", esDir.getRoot().getAbsolutePath());
    configs.put("path.data", esDir.getRoot().getAbsolutePath());
    configs.put("path.logs", esDir.getRoot().getAbsolutePath());
    esServer = createTestNode(configs);
    esServer.start();

    // Populate an index with some data

  }

  public static void cleanUp() throws Exception {
    if (esServer != null) {
      esServer.close();
    }
  }

  // this is needed in embedded mode.
  protected static void prepareElasticSearchServerForQueries() {
    esServer.client().admin().indices().prepareRefresh().execute().actionGet();
  }

  private static Node createTestNode(Map<String, Object> configs) {
    Settings.Builder settings = Settings.builder();
    for (Map.Entry<String, Object> config : configs.entrySet()) {
      settings.put(config.getKey(), config.getValue());
    }
    return new TestNode(settings.build());
  }

  @SuppressWarnings("unchecked")
  private static class TestNode extends Node {
    TestNode(Settings settings) {
      super(
          InternalSettingsPreparer.prepareEnvironment(settings, null),
          // To enable an http port in integration tests, the following plugin must be loaded.
          Collections.singletonList(Netty4Plugin.class)
      );
    }
  }
}
