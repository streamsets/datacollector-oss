/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.destination.solr;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.upgrader.YamlStageUpgrader;
import com.streamsets.pipeline.upgrader.YamlStageUpgraderLoader;
import com.streamsets.testing.pipeline.stage.TestUpgraderContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class TestSolrDTargetUpgrader {

  private static final String YAML_UPGRADER_PATH = "upgrader/SolrDTarget.yaml";
  private static final String DONT_CARE = "DONT_CARE";
  private static final String SPECIAL_VALUE = "SPECIAL_VALUE";

  private SolrDTargetUpgrader upgrader;
  private YamlStageUpgrader yamlUpgrader;

  @Before
  public void setUp() throws Exception {
    // Old Java upgrader
    upgrader = new SolrDTargetUpgrader();

    // New YAML upgrader
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource(YAML_UPGRADER_PATH);
    YamlStageUpgraderLoader loader = new YamlStageUpgraderLoader("stage", yamlResource);
    yamlUpgrader = loader.get();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testV1ToV2AddMissingField() {
    List< Config > validV1Configs = new ArrayList<Config>() {
      {
        add(new Config("instanceType", DONT_CARE));
        add(new Config("solrURI", DONT_CARE));
        add(new Config("zookeeperConnect", DONT_CARE));
        add(new Config("indexingMode", DONT_CARE));
        add(new Config("fieldNamesMap", DONT_CARE));
        add(new Config("missingFieldAction", DONT_CARE));
        add(new Config("kerberosAuth", DONT_CARE));
        add(new Config("skipValidation", DONT_CARE));
        add(new Config("stageOnRecordError", DONT_CARE));
        add(new Config("stageRequiredFields", DONT_CARE));
        add(new Config("stageRecordPreconditions", DONT_CARE));
      }
    };
    int oldNumberOfFields = validV1Configs.size();
    Set< String > fieldNames = validV1Configs.stream().map(Config::getName).collect(Collectors.toSet());
    // Add the new fields to the set
    fieldNames.add("defaultCollection");
    List<Config> newConfigs = upgrader.upgrade(
        validV1Configs,
        new TestUpgraderContext("library", "context", "stage", 1, 2)
    );
    // Check that the new configurations list has exactly one additional element
    assertEquals(oldNumberOfFields + 1, newConfigs.size());
    // Check that the field names are all correct
    for(Config config: newConfigs) {
      String configName = config.getName();
      assertTrue(fieldNames.contains(configName));
      // Check that no duplicates happened when upgrading
      fieldNames.remove(configName);
    }
  }

  @Test
  public void testV1ToV2PreserveAlreadyExistingField() {
    List< Config > validV2Configs = new ArrayList<Config>() {
      {
        add(new Config("instanceType", DONT_CARE));
        add(new Config("solrURI", DONT_CARE));
        add(new Config("zookeeperConnect", DONT_CARE));
        add(new Config("defaultCollection", SPECIAL_VALUE));
        add(new Config("indexingMode", DONT_CARE));
        add(new Config("fieldNamesMap", DONT_CARE));
        add(new Config("missingFieldAction", DONT_CARE));
        add(new Config("kerberosAuth", DONT_CARE));
        add(new Config("skipValidation", DONT_CARE));
        add(new Config("stageOnRecordError", DONT_CARE));
        add(new Config("stageRequiredFields", DONT_CARE));
        add(new Config("stageRecordPreconditions", DONT_CARE));
      }
    };
    int oldNumberOfFields = validV2Configs.size();
    Set< String > fieldNames = validV2Configs.stream().map(Config::getName).collect(Collectors.toSet());
    List<Config> newConfigs = upgrader.upgrade(
        validV2Configs,
        new TestUpgraderContext("library", "context", "stage", 1, 2)
    );
    // Check that the new configurations list has no additional elements
    assertEquals(oldNumberOfFields, newConfigs.size());
    // Check that the field names are all correct
    for(Config config: newConfigs) {
      String configName = config.getName();
      if(configName.equals("defaultCollection")) {
        // Check that the value of the defaultCollectionField was not replaced by the upgrader
        assertEquals(SPECIAL_VALUE, config.getValue());
      }
      assertTrue(fieldNames.contains(configName));
      // Check that no duplicates happened when upgrading
      fieldNames.remove(configName);
    }
  }

  @Test
  public void testV2ToV3AddMissingFields() {
    List< Config > validV2Configs = new ArrayList<Config>() {
      {
        add(new Config("instanceType", DONT_CARE));
        add(new Config("solrURI", DONT_CARE));
        add(new Config("zookeeperConnect", DONT_CARE));
        add(new Config("defaultCollection", DONT_CARE));
        add(new Config("indexingMode", DONT_CARE));
        add(new Config("fieldNamesMap", DONT_CARE));
        add(new Config("missingFieldAction", DONT_CARE));
        add(new Config("kerberosAuth", DONT_CARE));
        add(new Config("skipValidation", DONT_CARE));
        add(new Config("stageOnRecordError", DONT_CARE));
        add(new Config("stageRequiredFields", DONT_CARE));
        add(new Config("stageRecordPreconditions", DONT_CARE));
      }
    };
    int oldNumberOfFields = validV2Configs.size();
    Set< String > fieldNames = validV2Configs.stream().map(Config::getName).collect(Collectors.toSet());
    // Add the new fields to the set
    fieldNames.add("waitFlush");
    fieldNames.add("waitSearcher");
    fieldNames.add("softCommit");
    fieldNames.add("ignoreOptionalFields");
    List<Config> newConfigs = upgrader.upgrade(validV2Configs, new TestUpgraderContext("library", "context", "stage", 2, 3));
    // Check that the new configurations list has exactly four additional elements
    assertEquals(oldNumberOfFields + 4, newConfigs.size());
    // Check that the field names are all correct
    for(Config config: newConfigs) {
      String configName = config.getName();
      assertTrue(fieldNames.contains(configName));
      // Check that no duplicates happened when upgrading
      fieldNames.remove(configName);
    }
  }

  @Test
  public void testV3ToV4AddMissingFields() {
    List< Config > validV3Configs = new ArrayList<Config>() {
      {
        add(new Config("instanceType", DONT_CARE));
        add(new Config("solrURI", DONT_CARE));
        add(new Config("zookeeperConnect", DONT_CARE));
        add(new Config("defaultCollection", DONT_CARE));
        add(new Config("indexingMode", DONT_CARE));
        add(new Config("fieldNamesMap", DONT_CARE));
        add(new Config("ignoreOptionalFields", DONT_CARE));
        add(new Config("missingFieldAction", DONT_CARE));
        add(new Config("kerberosAuth", DONT_CARE));
        add(new Config("skipValidation", DONT_CARE));
        add(new Config("waitFlush", DONT_CARE));
        add(new Config("waitSearcher", DONT_CARE));
        add(new Config("softCommit", DONT_CARE));
        add(new Config("stageOnRecordError", DONT_CARE));
        add(new Config("stageRequiredFields", DONT_CARE));
        add(new Config("stageRecordPreconditions", DONT_CARE));
      }
    };
    int oldNumberOfFields = validV3Configs.size();
    Set< String > fieldNames = validV3Configs.stream().map(Config::getName).collect(Collectors.toSet());
    // Add the new fields to the set
    fieldNames.add("fieldsAlreadyMappedInRecord");
    fieldNames.add("recordSolrFieldsPath");
    List<Config> newConfigs = upgrader.upgrade(
        validV3Configs,
        new TestUpgraderContext("library", "context", "stage", 3, 4));
    // Check that the new configurations list has exactly two additional elements
    assertEquals(oldNumberOfFields + 2, newConfigs.size());
    // Check that the field names are all correct
    for(Config config: newConfigs) {
      String configName = config.getName();
      assertTrue(fieldNames.contains(configName));
      // Check that no duplicates happened when upgrading
      fieldNames.remove(configName);
    }
  }

  @Test
  public void testV4ToV5AddMissingFields() {
    List< Config > validV4Configs = new ArrayList<Config>() {
      {
        add(new Config("instanceType", DONT_CARE));
        add(new Config("solrURI", DONT_CARE));
        add(new Config("zookeeperConnect", DONT_CARE));
        add(new Config("defaultCollection", DONT_CARE));
        add(new Config("indexingMode", DONT_CARE));
        add(new Config("fieldNamesMap", DONT_CARE));
        add(new Config("ignoreOptionalFields", DONT_CARE));
        add(new Config("missingFieldAction", DONT_CARE));
        add(new Config("kerberosAuth", DONT_CARE));
        add(new Config("skipValidation", DONT_CARE));
        add(new Config("waitFlush", DONT_CARE));
        add(new Config("waitSearcher", DONT_CARE));
        add(new Config("softCommit", DONT_CARE));
        add(new Config("stageOnRecordError", DONT_CARE));
        add(new Config("stageRequiredFields", DONT_CARE));
        add(new Config("stageRecordPreconditions", DONT_CARE));
        add(new Config("fieldsAlreadyMappedInRecord", DONT_CARE));
        add(new Config("recordSolrFieldsPath", DONT_CARE));
      }
    };
    int oldNumberOfFields = validV4Configs.size();
    Set< String > fieldNames = validV4Configs.stream().map(Config::getName).collect(Collectors.toSet());
    // Add the new fields to the set
    fieldNames.add("autogeneratedFields");
    List<Config> newConfigs = upgrader.upgrade(
        validV4Configs,
        new TestUpgraderContext("library", "context", "stage", 4, 5));
    // Check that the new configurations list has exactly one additional element
    assertEquals(oldNumberOfFields + 1, newConfigs.size());
    for(Config config: newConfigs) {
      String configName = config.getName();
      assertTrue(fieldNames.contains(configName));
      // Check that no duplicates happened when upgrading
      fieldNames.remove(configName);
    }
  }

  @Test
  public void testV5ToV6AddPrefixToAlreadyExistingFields() {
    // Note that only the configuration parameters contained in the configBean must be
    // processed. This means that the rest of the parameters must not have the conf prefix added to their names
    Map<String, String> namesFromV5ToV6 = new HashMap< String, String >() {
      {
        put("instanceType", "conf.instanceType");
        put("solrURI", "conf.solrURI");
        put("zookeeperConnect", "conf.zookeeperConnect");
        put("defaultCollection", "conf.defaultCollection");
        put("indexingMode", "conf.indexingMode");
        put("fieldNamesMap", "conf.fieldNamesMap");
        put("ignoreOptionalFields", "conf.ignoreOptionalFields");
        put("missingFieldAction", "conf.missingFieldAction");
        put("kerberosAuth", "conf.kerberosAuth");
        put("skipValidation", "conf.skipValidation");
        put("waitFlush", "conf.waitFlush");
        put("waitSearcher", "conf.waitSearcher");
        put("softCommit", "conf.softCommit");
        put("autogeneratedFields", "conf.autogeneratedFields");
        put("stageOnRecordError", "stageOnRecordError");
        put("stageRequiredFields", "stageRequiredFields");
        put("stageRecordPreconditions", "stageRecordPreconditions");
        put("fieldsAlreadyMappedInRecord", "conf.fieldsAlreadyMappedInRecord");
        put("recordSolrFieldsPath", "conf.recordSolrFieldsPath");
      }
    };
    Map<String, String> namesFromV6ToV5 = new HashMap<String, String>() {
      {
        for(String key: namesFromV5ToV6.keySet()) {
          put(namesFromV5ToV6.get(key), key);
        }
      }
    };
    // Create a configuration such that all fields are like "solrURI": "solrURI"
    // If the V5 -> V6 upgrader works well we should receive a configuration like "conf.solrURI": "solrURI"
    // except for the stage fields, which should be left intact
    List< Config > validV5Configs = namesFromV5ToV6.keySet().stream().map(key -> new Config(key, key)).collect(
        Collectors.toList());
    // YAML upgrader is used from version 6 onwards
    List< Config > newConfigs =
        yamlUpgrader.upgrade(validV5Configs, new TestUpgraderContext("lib", "stage", "instance", 5, 6));
    Set< String > observedKeys = new HashSet<>();
    for(Config config: newConfigs) {
      String receivedName = config.getName();
      assertTrue(namesFromV6ToV5.containsKey(receivedName));
      assertEquals(config.getValue(), namesFromV6ToV5.get(receivedName));
      observedKeys.add(receivedName);
    }
    // Check that we have observed all the keys (i.e: we do not have cases like ["key1", "key1", "key2"]
    assertEquals(namesFromV6ToV5.keySet(), observedKeys);
    // Check that no extra field has been added
    assertEquals(validV5Configs.size(), newConfigs.size());

  }

  @Test
  public void testV6ToV7AddMissingFields() {
    List<Config> validV6Configs = new ArrayList<Config>() {
      {
        add(new Config("conf.zookeeperConnect", DONT_CARE));
        add(new Config("stageRequiredFields", DONT_CARE));
        add(new Config("conf.fieldNamesMap", DONT_CARE));
        add(new Config("conf.softCommit", DONT_CARE));
        add(new Config("conf.solrURI", DONT_CARE));
        add(new Config("conf.waitSearcher", DONT_CARE));
        add(new Config("conf.recordSolrFieldsPath", DONT_CARE));
        add(new Config("stageOnRecordError", DONT_CARE));
        add(new Config("conf.indexingMode", DONT_CARE));
        add(new Config("conf.waitFlush", DONT_CARE));
        add(new Config("conf.instanceType", DONT_CARE));
        add(new Config("conf.kerberosAuth", DONT_CARE));
        add(new Config("conf.missingFieldAction", DONT_CARE));
        add(new Config("conf.defaultCollection", DONT_CARE));
        add(new Config("conf.ignoreOptionalFields", DONT_CARE));
        add(new Config("conf.skipValidation", DONT_CARE));
        add(new Config("conf.autogeneratedFields", DONT_CARE));
        add(new Config("conf.fieldsAlreadyMappedInRecord", DONT_CARE));
        add(new Config("stageRecordPreconditions", DONT_CARE));
      }
    };
    int oldNumberOfFields = validV6Configs.size();
    Set< String > fieldNames = validV6Configs.stream().map(Config::getName).collect(Collectors.toSet());
    // Add the new fields to the set
    fieldNames.add("conf.connectionTimeout");
    fieldNames.add("conf.socketTimeout");
    List<Config> newConfigs = yamlUpgrader.upgrade(
        validV6Configs,
        new TestUpgraderContext("lib", "stage", "instance", 6, 7)
    );
    // Check that the new configurations list has exactly two additional elements
    assertEquals(oldNumberOfFields + 2, newConfigs.size());
    // Check that the field names are all correct
    for(Config config: newConfigs) {
      String configName = config.getName();
      assertTrue(fieldNames.contains(configName));
      // Check that no duplicates happened when upgrading
      fieldNames.remove(configName);
    }
  }

}