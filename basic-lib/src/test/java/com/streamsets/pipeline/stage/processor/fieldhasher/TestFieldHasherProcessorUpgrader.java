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
package com.streamsets.pipeline.stage.processor.fieldhasher;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

/**
 * Test the FieldHasherProcessorUpgrader.
 */
public class TestFieldHasherProcessorUpgrader {

  private String getNotPresentConfigs(Set<String> configsNotPresentAfterUpgrade) {
    StringBuilder sb = new StringBuilder();
    boolean isStart = true;
    for (String config : configsNotPresentAfterUpgrade) {
      if (!isStart) {
        sb.append(",");
      }
      sb.append(config);
      isStart = false;
    }
    return sb.toString();
  }

  @Test
  public void testUpgradeV1toV2() throws StageException {
    //Old Config
    final String FIELD_HASHER_CONFIG = "fieldHasherConfigs";

    //New Config
    final Joiner JOINER = Joiner.on(".");
    //v1 to v2 constants
    //old fields
    final String FIELD_HASHER_CONFIGS = "fieldHasherConfigs";
    final String FIELDS_TO_HASH = "fieldsToHash";
    //New fields
    final String HASHER_CONFIG = "hasherConfig";
    final String RECORD_HASHER_CONFIG = "recordHasherConfig";
    final String TARGET_FIELD_HASHER_CONFIGS = "targetFieldHasherConfigs";
    final String INPLACE_FIELD_HASHER_CONFIGS = "inPlaceFieldHasherConfigs";
    final String SOURCE_FIELDS_TO_HASH = "sourceFieldsToHash";
    final String HASH_ENTIRE_RECORD = "hashEntireRecord";
    final String HASH_TYPE = "hashType";
    final String TARGET_FIELD = "targetField";
    final String HEADER_ATTRIBUTE = "headerAttribute";
    final String INCLUDE_RECORD_HEADER = "includeRecordHeaderForHashing";


    List<Config> configs = new ArrayList<>();
    LinkedHashMap<String, Object> fieldHasherConfig1 = new LinkedHashMap<String, Object>();
    fieldHasherConfig1.put("fieldsToHash", ImmutableList.of("/a", "/b"));
    fieldHasherConfig1.put("hashType", HashType.MD5);

    LinkedHashMap<String, Object> fieldHasherConfig2 = new LinkedHashMap<String, Object>();
    fieldHasherConfig2.put("fieldsToHash", ImmutableList.of("/c", "/d"));
    fieldHasherConfig2.put("hashType", HashType.SHA1);

    List<LinkedHashMap<String, Object>> fieldHasherConfigs = new ArrayList<LinkedHashMap<String, Object>>();
    configs.add(new Config(FIELD_HASHER_CONFIG, fieldHasherConfigs));
    fieldHasherConfigs.add(fieldHasherConfig1);
    fieldHasherConfigs.add(fieldHasherConfig2);

    FieldHasherProcessorUpgrader upgrader = new FieldHasherProcessorUpgrader();
    upgrader.upgrade("a", "b", "c", 1, 2, configs);

    Set<String> configsToBePresentAfterUpgrade = new HashSet<String>();
    configsToBePresentAfterUpgrade.add(JOINER.join(HASHER_CONFIG, RECORD_HASHER_CONFIG, HASH_ENTIRE_RECORD));
    configsToBePresentAfterUpgrade.add(JOINER.join(HASHER_CONFIG, RECORD_HASHER_CONFIG, INCLUDE_RECORD_HEADER));
    configsToBePresentAfterUpgrade.add(JOINER.join(HASHER_CONFIG, RECORD_HASHER_CONFIG, HASH_TYPE));
    configsToBePresentAfterUpgrade.add(JOINER.join(HASHER_CONFIG, RECORD_HASHER_CONFIG, TARGET_FIELD));
    configsToBePresentAfterUpgrade.add(JOINER.join(HASHER_CONFIG, RECORD_HASHER_CONFIG, HEADER_ATTRIBUTE));

    configsToBePresentAfterUpgrade.add(JOINER.join(HASHER_CONFIG, INPLACE_FIELD_HASHER_CONFIGS));
    configsToBePresentAfterUpgrade.add(JOINER.join(HASHER_CONFIG, TARGET_FIELD_HASHER_CONFIGS));

    // previously this was 7.  now it's 9 due to adding 2 more configs
    // for SDC-6540 which added the "useSeparator" options.
    // the upgrader runs both V1 to V2 then the V2 to V3 upgrades.
    Assert.assertEquals("There should be 9 configs after upgrade", configs.size(), 9);

    for (Config config : configs) {

      if (config.getName() == JOINER.join(HASHER_CONFIG, RECORD_HASHER_CONFIG, HASH_ENTIRE_RECORD)) {

        Assert.assertFalse("Record Hashing should be disabled after upgrade",
            ((Boolean)config.getValue()).booleanValue());

      } else if (config.getName() == JOINER.join(HASHER_CONFIG, INPLACE_FIELD_HASHER_CONFIGS)) {

        List upgradedInPlaceHasherConfigs = (List)config.getValue();
        Assert.assertEquals(
            "After upgrade the number of field hash configs should be same", upgradedInPlaceHasherConfigs.size(), 2);

        for(Object upgradedFieldHasherConfigObject : upgradedInPlaceHasherConfigs) {
          LinkedHashMap<String, Object> upgradedFieldHasherConfig =
              (LinkedHashMap<String, Object>) upgradedFieldHasherConfigObject;
          Assert.assertTrue("InPlace Field Hasher Config should contain source Fields To Hash And Hash Type",
              upgradedFieldHasherConfig.containsKey("sourceFieldsToHash") &&
                  upgradedFieldHasherConfig.containsKey("hashType"));

        }

      } else if (config.getName() == JOINER.join(HASHER_CONFIG, TARGET_FIELD_HASHER_CONFIGS)) {

        List upgradedTargetHasherConfigs = (List)config.getValue();
        Assert.assertEquals(
            "After upgrade the number of target field hash configs should be 0",
            upgradedTargetHasherConfigs.size(),
            0
        );

      }

      configsToBePresentAfterUpgrade.remove(config.getName());
    }

    Assert.assertTrue(
        "After upgrade the following fields are not present" +
            getNotPresentConfigs(configsToBePresentAfterUpgrade),
        configsToBePresentAfterUpgrade.isEmpty()
    );
  }

  @Test
  public void testUpgradeV2ToV3() throws StageException {
    final Joiner JOINER = Joiner.on(".");

    final String HASHER_CONFIGS = "hasherConfig";
    final String RECORD_HASHER_CONFIGS = "recordHasherConfig";

    // v2 to v3 added this field - must be set to true.
    final String USE_SEPARATOR = "useSeparator";

    List<Config> configs = new ArrayList<>();

    FieldHasherProcessorUpgrader upgrader = new FieldHasherProcessorUpgrader();
    upgrader.upgrade("a", "b", "c", 2, 3, configs);

    Assert.assertEquals("Incorrect number of configs after upgrade", configs.size(), 2);

    for (Config config : configs) {
      if (config.getName().equals(JOINER.join(HASHER_CONFIGS, RECORD_HASHER_CONFIGS, USE_SEPARATOR))) {
        Assert.assertTrue("UseSeparator Should be true", ((Boolean)config.getValue()).booleanValue());
      } else if (config.getName().equals(JOINER.join(HASHER_CONFIGS, USE_SEPARATOR))) {
        Assert.assertTrue("UseSeparator Should be true", ((Boolean)config.getValue()).booleanValue());
      }
    }
  }
}
