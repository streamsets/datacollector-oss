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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper.findByName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataFormatUpgradeHelperTest {
  private static final String prefix = "conf.dataFormatConfig";

  @Test
  public void upgradeAvroParserWithSchemaRegistrySupport() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config(prefix + ".schemaInMessage", true));
    configs.add(new Config(prefix + ".avroSchema", ""));

    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);

    assertEquals(6, configs.size());

    assertEquals(
        "SOURCE",
        findByName(configs, "avroSchemaSource").get().getValue()
    );
    checkStringConfig(configs, "avroSchema");
    checkConfig(configs, "schemaLookupMode", String.class);
    checkListConfig(configs, "schemaRegistryUrls");
    checkStringConfig(configs, "subject");
    checkConfig(configs, "schemaId", Integer.class);
  }

  @Test
  public void upgradeAvroParserWithSchemaRegistrySupportWithoutSchemaInMessage() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config(prefix + "avroSchema", ""));

    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);
    assertEquals("SOURCE", findByName(configs, "avroSchemaSource").get().getValue());
  }

  @Test
  public void upgradeAvroParserWithSchemaRegistrySupportWithSchemaInMessageSetToFalse() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config(prefix + ".avroSchemaInHeader", false));
    configs.add(new Config(prefix + ".avroSchema", ""));

    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);
    assertEquals("SOURCE", findByName(configs, "avroSchemaSource").get().getValue());
  }

  @Test
  public void upgradeAvroGeneratorWithSchemaRegistrySupport() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config(prefix + ".avroSchemaInHeader", true));
    configs.add(new Config(prefix + ".avroSchema", ""));
    configs.add(new Config(prefix + ".includeSchema", false));
    configs.add(new Config(prefix + ".avroCompression", "NULL"));

    DataFormatUpgradeHelper.upgradeAvroGeneratorWithSchemaRegistrySupport(configs);

    assertEquals(11, configs.size());

    assertEquals("HEADER", findByName(configs, "avroSchemaSource").get().getValue());

    checkStringConfig(configs, "avroSchema");
    checkConfig(configs, "schemaLookupMode", String.class);
    checkListConfig(configs, "schemaRegistryUrls");
    checkListConfig(configs, "schemaRegistryUrlsForRegistration");
    checkStringConfig(configs, "subject");
    checkStringConfig(configs, "subjectToRegister");
    checkConfig(configs, "schemaId", Integer.class);
    checkBooleanConfig(configs, "includeSchema");
    checkBooleanConfig(configs, "registerSchema");
  }

  @Test
  public void upgradeAvroGeneratorWithSchemaRegistrySupportWithoutSchemaInMessage() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config(prefix + "avroSchema", ""));

    DataFormatUpgradeHelper.upgradeAvroGeneratorWithSchemaRegistrySupport(configs);
    assertEquals("INLINE", findByName(configs, "avroSchemaSource").get().getValue());
  }

  @Test
  public void upgradeAvroGeneratorWithSchemaRegistrySupportWithSchemaInMessageSetToFalse() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config(prefix + ".avroSchemaInHeader", false));
    configs.add(new Config(prefix + ".avroSchema", ""));

    DataFormatUpgradeHelper.upgradeAvroGeneratorWithSchemaRegistrySupport(configs);
    assertEquals("INLINE", findByName(configs, "avroSchemaSource").get().getValue());
  }

  private void checkConfig(List<Config> configs, String name, Class type) {
    Optional<Config> config = findByName(configs, name);
    assertTrue(config.isPresent());
    assertTrue(type.isInstance(config.get().getValue()));
  }

  private void checkStringConfig(List<Config> configs, String name) {
    checkConfig(configs, name, String.class);
  }

  private void checkListConfig(List<Config> configs, String name) {
    checkConfig(configs, name, List.class);
  }

  private void checkBooleanConfig(List<Config> configs, String name) {
    checkConfig(configs, name, Boolean.class);
  }
}
