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
package com.streamsets.datacollector.lineage;

import com.google.common.base.Joiner;

public class LineagePublisherConstants {

  /**
   * Configuration property in sdc.properties with the declared publisher plugins ids.
   */
  public static final String CONFIG_LINEAGE_PUBLISHERS = "lineage.publishers";

  /**
   * Maximal number of lineage events that will be cached in memory (and thus lost on crash).
   */
  public static final String CONFIG_LINEAGE_QUEUE_SIZE = "lineage.queue.size";
  public static final int DEFAULT_LINEAGE_QUEUE_SIZE = 100;

  /**
   * Each publisher have it's own configuration space, so the prefixes/postfixes are private and one should use
   * the access methods that will resolve the property for given publisher name.
   */
  // Prefix of all named publisher keys
  private static final String CONFIG_LINEAGE_PUBLISHER_PREFIX = "lineage.publisher";
  // Definition of the plugin (stage library name and publisher name)
  private static final String CONFIG_LINEAGE_PUBSLIHER_DEF = "def";
  // Configuration properties
  private static final String CONFIG_LINEAGE_PUBSLIHER_CONFIG = "config.";
  private static Joiner dot = Joiner.on(".");

  /**
   * Definition of the publisher (stage library and name).
   */
  public static String configDef(String name) {
    return dot.join(CONFIG_LINEAGE_PUBLISHER_PREFIX, name, CONFIG_LINEAGE_PUBSLIHER_DEF);
  }

  /**
   * Custom configuration space for each publisher.
   */
  public static String configConfig(String name) {
    return dot.join(CONFIG_LINEAGE_PUBLISHER_PREFIX, name, CONFIG_LINEAGE_PUBSLIHER_CONFIG);
  }

  private LineagePublisherConstants() {
    // Instantiation is prohibited
  }
}
