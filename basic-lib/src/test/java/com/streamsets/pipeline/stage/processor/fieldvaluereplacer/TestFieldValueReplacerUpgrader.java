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
package com.streamsets.pipeline.stage.processor.fieldvaluereplacer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.Config;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class TestFieldValueReplacerUpgrader {
  @Test
  @SuppressWarnings("unchecked")
  public void testUpgradeV1ToV2WithoutConditionalValueReplace() throws Exception {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config(FieldValueReplacerUpgrader.FIELDS_TO_NULL, ImmutableList.of("/a", "/b", "/c")));
    FieldValueReplacerUpgrader upgrader = new FieldValueReplacerUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("", "", "", 1, 2, configs);
    Assert.assertEquals(2, upgradedConfigs.size());

    Set<String> shouldExistConfigNames = ImmutableSet.of(FieldValueReplacerUpgrader.FIELDS_TO_NULL, FieldValueReplacerUpgrader.FIELDS_TO_CONDITIONALLY_REPLACE);
    Set<String> upgradedConfigNames = new HashSet<>();
    for (Config config : upgradedConfigs) {
      upgradedConfigNames.add(config.getName());
      if (config.getName().equals(FieldValueReplacerUpgrader.FIELDS_TO_CONDITIONALLY_REPLACE)) {
        List<LinkedHashMap<String, Object>> configValue = (List<LinkedHashMap<String,Object>>) config.getValue();
        Assert.assertTrue(configValue.isEmpty());
      }
    }
    Assert.assertEquals(shouldExistConfigNames.size(), upgradedConfigNames.size());
    Assert.assertTrue(Sets.difference(shouldExistConfigNames, upgradedConfigNames).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUpgradeV1ToV2WithConditionalValueReplace() throws Exception {
    List<Config> configs = new ArrayList<>();

    List<LinkedHashMap<String, Object>> fieldsToConditionallyReplaceListBean = new ArrayList<>();

    LinkedHashMap<String, Object> fieldsToConditionallyReplace1 = new LinkedHashMap<>();
    fieldsToConditionallyReplace1.put("fieldNames", ImmutableList.of("/d", "/e", "/f"));
    fieldsToConditionallyReplace1.put("operator", OperatorChooserValues.LESS_THAN);
    fieldsToConditionallyReplace1.put("comparisonValue", 0);
    fieldsToConditionallyReplace1.put("replacementValue", 0);

    LinkedHashMap<String, Object> fieldsToConditionallyReplace2 = new LinkedHashMap<>();
    fieldsToConditionallyReplace2.put("fieldNames", ImmutableList.of("/g", "/h", "/i"));
    fieldsToConditionallyReplace2.put("operator", OperatorChooserValues.GREATER_THAN);
    fieldsToConditionallyReplace2.put("comparisonValue", 10);
    fieldsToConditionallyReplace2.put("replacementValue", 0);

    fieldsToConditionallyReplaceListBean.add(fieldsToConditionallyReplace1);
    fieldsToConditionallyReplaceListBean.add(fieldsToConditionallyReplace2);

    configs.add(new Config(FieldValueReplacerUpgrader.FIELDS_TO_NULL, ImmutableList.of("/a", "/b", "/c")));
    configs.add(new Config(FieldValueReplacerUpgrader.FIELDS_TO_CONDITIONALLY_REPLACE, fieldsToConditionallyReplaceListBean));

    FieldValueReplacerUpgrader upgrader = new FieldValueReplacerUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("", "", "", 1, 2, configs);
    Assert.assertEquals(2, upgradedConfigs.size());

    Set<String> shouldExistConfigNames = ImmutableSet.of(FieldValueReplacerUpgrader.FIELDS_TO_NULL, FieldValueReplacerUpgrader.FIELDS_TO_CONDITIONALLY_REPLACE);
    Set<String> upgradedConfigNames = new HashSet<>();
    for (Config config : upgradedConfigs) {
      upgradedConfigNames.add(config.getName());
      if (config.getName().equals(FieldValueReplacerUpgrader.FIELDS_TO_CONDITIONALLY_REPLACE)) {
        List<LinkedHashMap<String, Object>> configValue = (List<LinkedHashMap<String,Object>>) config.getValue();
        Assert.assertEquals(2, configValue.size());
        for (int i = 0 ; i < 2; i++) {
          LinkedHashMap<String, Object> expectedLinkedHashMap = fieldsToConditionallyReplaceListBean.get(i);
          LinkedHashMap<String, Object> actualLinkedHashMap = configValue.get(i);
          Assert.assertEquals(expectedLinkedHashMap.keySet(), actualLinkedHashMap.keySet());
          for (String key : expectedLinkedHashMap.keySet()) {
            Object expected = expectedLinkedHashMap.get(key);
            Object actual = actualLinkedHashMap.get(key);
            Assert.assertEquals(expected.getClass(), actual.getClass());
            if (List.class.isAssignableFrom(expected.getClass())) {
              Assert.assertEquals(((List)expected).size(), ((List)actual).size());
              Assert.assertArrayEquals(((List)expected).toArray(), ((List)actual).toArray());
            } else {
              Assert.assertEquals(expected, actual);
            }
          }
        }
      }
    }
    Assert.assertEquals(shouldExistConfigNames.size(), upgradedConfigNames.size());
    Assert.assertTrue(Sets.difference(shouldExistConfigNames, upgradedConfigNames).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUpgradeV2ToV3() throws Exception {
    List<Config> configs = new ArrayList<>();
    List<String> fieldsToNull = ImmutableList.of("/a", "/b", "/c");
    configs.add(new Config(FieldValueReplacerUpgrader.FIELDS_TO_NULL, fieldsToNull));

    List<LinkedHashMap<String, Object>> fieldsToConditionallyReplaceListBean = new ArrayList<>();

    LinkedHashMap<String, Object> fieldsToConditionallyReplace = new LinkedHashMap<>();
    fieldsToConditionallyReplace.put("fieldNames", ImmutableList.of("/d", "/e", "/f"));
    fieldsToConditionallyReplace.put("operator", OperatorChooserValues.LESS_THAN);
    fieldsToConditionallyReplace.put("comparisonValue", 0);
    fieldsToConditionallyReplace.put("replacementValue", 0);

    fieldsToConditionallyReplaceListBean.add(fieldsToConditionallyReplace);

    configs.add(new Config(FieldValueReplacerUpgrader.FIELDS_TO_CONDITIONALLY_REPLACE, fieldsToConditionallyReplaceListBean));

    FieldValueReplacerUpgrader upgrader = new FieldValueReplacerUpgrader();

    List<Config> upgradedConfigs = upgrader.upgrade("", "", "", 2, 3, configs);
    Assert.assertEquals(2, upgradedConfigs.size());

    Set<String> shouldExistConfigNames = ImmutableSet.of(FieldValueReplacerUpgrader.NULL_REPLACER_CONFIGS, FieldValueReplacerUpgrader.FIELDS_TO_CONDITIONALLY_REPLACE);
    Set<String> upgradedConfigNames = new HashSet<>();
    for (Config config : upgradedConfigs) {
      upgradedConfigNames.add(config.getName());
      if (config.getName().equals(FieldValueReplacerUpgrader.FIELDS_TO_CONDITIONALLY_REPLACE)) {
        List<LinkedHashMap<String, Object>> configValue = (List<LinkedHashMap<String,Object>>) config.getValue();
        Assert.assertEquals(1, configValue.size());
        LinkedHashMap<String, Object> conditionallReplaceConfigValue = configValue.get(0);
        Assert.assertTrue(conditionallReplaceConfigValue.keySet().containsAll(ImmutableSet.of("fieldNames", "operator", "comparisonValue", "replacementValue")));
      } else if(config.getName().equals(FieldValueReplacerUpgrader.NULL_REPLACER_CONFIGS)) {
        List<LinkedHashMap<String, Object>> configValue = (List<LinkedHashMap<String,Object>>) config.getValue();
        Assert.assertEquals(1, configValue.size());
        LinkedHashMap<String, Object> nullReplacerConfig = configValue.get(0);
        Assert.assertTrue(nullReplacerConfig.keySet().containsAll(ImmutableSet.of(FieldValueReplacerUpgrader.FIELDS_TO_NULL, FieldValueReplacerUpgrader.NULL_REPLACER_CONFIG_CONDITION)));
        Assert.assertTrue( ((String) nullReplacerConfig.get(FieldValueReplacerUpgrader.NULL_REPLACER_CONFIG_CONDITION)).isEmpty());
        List<String> actualFieldsToNull = (List<String>) nullReplacerConfig.get(FieldValueReplacerUpgrader.FIELDS_TO_NULL);
        Assert.assertEquals(fieldsToNull, actualFieldsToNull);
      }
    }
    Assert.assertEquals(shouldExistConfigNames.size(), upgradedConfigNames.size());
    Assert.assertTrue(Sets.difference(shouldExistConfigNames, upgradedConfigNames).isEmpty());
  }
}
