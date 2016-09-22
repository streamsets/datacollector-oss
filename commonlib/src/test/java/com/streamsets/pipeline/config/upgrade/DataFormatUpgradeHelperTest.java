/*
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.config.upgrade;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.config.AvroCompression;
import com.streamsets.pipeline.config.DestinationAvroSchemaSource;
import com.streamsets.pipeline.config.OriginAvroSchemaSource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataFormatUpgradeHelperTest {
  private static final String prefix = "conf.dataFormatConfig";
  private static final Joiner PERIOD = Joiner.on(".");

  @Test
  public void upgradeAvroParserWithSchemaRegistrySupport() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config(PERIOD.join(prefix, "schemaInMessage"), true));
    configs.add(new Config(PERIOD.join(prefix, "avroSchema"), ""));

    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);

    assertEquals(6, configs.size());

    assertEquals(
        OriginAvroSchemaSource.SOURCE,
        DataFormatUpgradeHelper.findByName(configs, "avroSchemaSource").get().getValue()
    );
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "avroSchema").isPresent());
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "schemaLookupMode").isPresent());
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "schemaRegistryUrls").isPresent());
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "subject").isPresent());
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "schemaId").isPresent());
  }

  @Test
  public void upgradeAvroGeneratorWithSchemaRegistrySupport() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config(PERIOD.join(prefix, "avroSchemaInHeader"), true));
    configs.add(new Config(PERIOD.join(prefix, "avroSchema"), ""));
    configs.add(new Config(PERIOD.join(prefix, "includeSchema"), false));
    configs.add(new Config(PERIOD.join(prefix, "avroCompression"), AvroCompression.NULL));

    DataFormatUpgradeHelper.upgradeAvroGeneratorWithSchemaRegistrySupport(configs);

    assertEquals(11, configs.size());

    assertEquals(
        DestinationAvroSchemaSource.HEADER,
        DataFormatUpgradeHelper.findByName(configs, "avroSchemaSource").get().getValue()
    );
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "avroSchema").isPresent());
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "schemaLookupMode").isPresent());
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "schemaRegistryUrls").isPresent());
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "subject").isPresent());
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "subjectToRegister").isPresent());
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "schemaId").isPresent());
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "includeSchema").isPresent());
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "registerSchema").isPresent());
    assertTrue(DataFormatUpgradeHelper.findByName(configs, "schemaRegistryUrlsForRegistration").isPresent());
    assertEquals(
        "None",
        ((AvroCompression) DataFormatUpgradeHelper.findByName(configs, "avroCompression").get().getValue()).getLabel()
    );
  }
}