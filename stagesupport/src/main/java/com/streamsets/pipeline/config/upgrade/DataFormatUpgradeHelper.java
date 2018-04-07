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
package com.streamsets.pipeline.config.upgrade;

import com.streamsets.pipeline.api.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Utility class for extracting common data format upgrade code
 * that can be used in individual stage upgraders until ConfigBean upgrading
 * is possible.
 */
public class DataFormatUpgradeHelper {

  private DataFormatUpgradeHelper() {
  }

  public static void upgradeAvroParserWithSchemaRegistrySupport(List<Config> configs) {
    List<Config> toRemove = new ArrayList<>();
    List<Config> toAdd = new ArrayList<>();

    Optional<Config> avroSchema = findByName(configs, "avroSchema");
    if (!avroSchema.isPresent()) {
      throw new IllegalStateException("Config 'avroSchema' is missing, this upgrader cannot be applied.");
    }

    String configName = avroSchema.get().getName();
    String prefix = configName.substring(0, configName.lastIndexOf("."));

    // schemaInMessage was removed and superseded by OriginAvroSchemaSource.SOURCE
    Optional<Config> schemaInMessage = findByName(configs, "schemaInMessage");

    // This upgrader intentionally behaves differently then plain new stage on the canvas. On new stage user is forced
    // to chose where is the Avro schema as this decision is no longer "simple". However for people who are upgrading
    // we're making the same decision that they selected in the past.
    if (schemaInMessage.isPresent()) {
      if ((boolean) schemaInMessage.get().getValue()) {
        toAdd.add(new Config(prefix + ".avroSchemaSource", "SOURCE"));
      } else {
        toAdd.add(new Config(prefix + ".avroSchemaSource", "INLINE"));
      }
      toRemove.add(schemaInMessage.get());
    } else {
      toAdd.add(new Config(prefix + ".avroSchemaSource", "SOURCE"));
    }

    // New configs added
    toAdd.add(new Config(prefix + ".schemaRegistryUrls", new ArrayList<>()));
    toAdd.add(new Config(prefix + ".schemaLookupMode", "AUTO"));
    toAdd.add(new Config(prefix + ".subject", ""));
    toAdd.add(new Config(prefix + ".schemaId", 0));

    configs.removeAll(toRemove);
    configs.addAll(toAdd);
  }

  public static void upgradeAvroGeneratorWithSchemaRegistrySupport(List<Config> configs) {
    List<Config> toRemove = new ArrayList<>();
    List<Config> toAdd = new ArrayList<>();

    Optional<Config> avroSchema = findByName(configs, "avroSchema");

    if (!avroSchema.isPresent()) {
      throw new IllegalStateException("Config 'avroSchema' is missing, this upgrader cannot be applied.");
    }

    String configName = avroSchema.get().getName();
    String prefix = configName.substring(0, configName.lastIndexOf("."));

    // avroSchemaInHeader was removed and superseded by DestinationAvroSchemaSource.HEADER
    Optional<Config> avroSchemaInHeader = findByName(configs, "avroSchemaInHeader");
    if (avroSchemaInHeader.isPresent()) {
      if ((boolean) avroSchemaInHeader.get().getValue()) {
        toAdd.add(new Config(prefix + ".avroSchemaSource", "HEADER"));
      } else {
        toAdd.add(new Config(prefix + ".avroSchemaSource", "INLINE"));
      }
      toRemove.add(avroSchemaInHeader.get());
    } else {
      toAdd.add(new Config(prefix + ".avroSchemaSource", "INLINE"));
    }

    toAdd.add(new Config(prefix + ".registerSchema", false));
    toAdd.add(new Config(prefix + ".schemaRegistryUrlsForRegistration", new ArrayList<>()));
    toAdd.add(new Config(prefix + ".schemaRegistryUrls", new ArrayList<>()));
    toAdd.add(new Config(prefix + ".schemaLookupMode", "AUTO"));
    toAdd.add(new Config(prefix + ".subject", ""));
    toAdd.add(new Config(prefix + ".subjectToRegister", ""));
    toAdd.add(new Config(prefix + ".schemaId", 0));

    configs.removeAll(toRemove);
    configs.addAll(toAdd);
  }

  /**
   * There is a problem with some older stages that originally were not using our data parser library as the
   * upgrade on those stages does not create all the properties that were introduced by the data parser library.
   * For example File Tail origin doesn't support avro, so the upgrade procedure doesn't use create the avro
   * specific properties. This is normally not a problem as post-upgrade SDC will create all missing properties
   * with default values. However this particular upgrade will fail if the property avroSchema is missing.
   *
   * Hence for such stages, this method will ensure that the property avroSchema is properly present. The migration
   * to data parser library happened for majority of stages in 1.2 release and hence this really affects only people
   * upgrading from 1.1.x all the way to 2.1 or above. If the user upgraded from 1.1.x and then to any other release
   * below 2.1 first, they would not hit this issue as the property avroSchema would be added with default value.
   */
  public static void ensureAvroSchemaExists(List<Config> configs, String prefix) {
    Optional<Config> avroSchema = findByName(configs, "avroSchema");
    if (!avroSchema.isPresent()) {
      configs.add(new Config(prefix + ".avroSchema", null));
    }
  }

  static Optional<Config> findByName(List<Config> configs, String name) {
    for (Config config : configs) {
      if (config.getName().endsWith(name)) {
        return Optional.of(config);
      }
    }
    return Optional.empty();
  }
}
