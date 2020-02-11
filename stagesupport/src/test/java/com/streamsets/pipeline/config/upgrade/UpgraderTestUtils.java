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
import org.junit.Assert;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test utilities for {@link UpgraderUtils} functionality
 */
public class UpgraderTestUtils {

  /**
   * A class that tracks the state of {@link Config} objects across an upgrade invocation
   */
  public static class UpgradeMoveWatcher {
    private final Map<String, Object> oldNamesToValues = new HashMap<>();

    private UpgradeMoveWatcher(List<Config> configs) {
      for (Config config : configs) {
        oldNamesToValues.put(config.getName(), config.getValue());
      }
    }

    /**
     * <p>
     *   Asserts that all {@link Config} objects have been moved from old names to new names, with values preserved.
     *   Assertion will fail if any old names are still present in the current (passed) configs, or if any new names
     *   are not present.
     * </p>
     *
     * @param configs list of current (post-upgrade) configs
     * @param oldToNewNames map of old to new config names
     */
    public void assertAllMoved(List<Config> configs, Map<String, String> oldToNewNames) {
      Set<String> newNamesSeen = new HashSet<>(oldToNewNames.values());
      Map<String, String> newToOld = oldToNewNames.entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

      for (Config config : configs) {
        final String name = config.getName();
        if (oldToNewNames.containsKey(name)) {
          Assert.fail(String.format(
              "configs contained %s, which should have been moved to %s",
              name,
              oldToNewNames.get(name))
          );
        }
        if (newNamesSeen.contains(name)) {
          Assert.assertEquals(
              String.format(
                  "config value %s not moved correctly to new name %s; new value is %s",
                  oldNamesToValues.get(name),
                  name,
                  config.getValue()
              ),
              oldNamesToValues.get(newToOld.get(name)),
              config.getValue()
          );
          Assert.assertTrue(newNamesSeen.remove(name));
        }
      }

      Assert.assertEquals(
          String.format(
              "new configs did not contain expected new names: %s",
              String.join(", ", newNamesSeen)
          ),
          0,
          newNamesSeen.size()
      );
    }


    /**
     * <p>
     *   Asserts that all {@link Config} objects have been moved from old names to new names, with values preserved.
     *   Assertion will fail if any old names are still present in the current (passed) configs, or if any new names
     *   are not present.
     * </p>
     *
     * @param configs list of current (post-upgrade) configs
     * @param names mapping of old to new names, in the form of [oldName1, newName1, oldName2, newName2, ...]
     */
    public void assertAllMoved(List<Config> configs, String... names) {

      Map<String, String> nameMap = new HashMap<>();
      if (names.length % 2 == 1) {
        throw new IllegalArgumentException("names was of uneven length");
      }
      for (int i=0; i<names.length; ) {
        nameMap.put(names[i], names[i+1]);
        i+=2;
      }
      assertAllMoved(configs, nameMap);
    }
  }

  /**
   * Ensures that none of the passed propertyNames exist in the configs.  Causes assertion error if any do.
   *
   * @param configs the list of configs
   * @param propertyNames property names to check
   */
  public static void assertNoneExist(List<Config> configs, String... propertyNames) {
    Set<String> allPropertyNames = new HashSet<>(Arrays.asList(propertyNames));
    for (Config config : configs) {
      if (allPropertyNames.contains(config.getName())) {
        Assert.fail(String.format(
            "Property name %s was contained in configs (with value %s) but should not have been",
            config.getName(),
            config.getValue().toString()
        ));
      }
    }
  }

  /**
   * Ensures that all of the passed propertyNames exist in the configs.  Causes assertion error if any do not.
   *
   * @param configs the list of configs
   * @param propertyNames property names to check
   */
  public static void assertAllExist(List<Config> configs, String... propertyNames) {
    Set<String> allPropertyNames = new HashSet<>(Arrays.asList(propertyNames));
    configs.forEach(config -> allPropertyNames.remove(config.getName()));
    assertThat(String.format(
        "Expected all properties %s to exist in configs, but these were not present: %s",
        String.join(", ", propertyNames),
        String.join(", ", allPropertyNames)
    ), allPropertyNames, empty());
  }

  public static void assertExists(List<Config> configs, String propertyName, Object propertyValue) {
    for (Config config : configs) {
      if (config.getName().equals(propertyName)) {
        assertThat(config.getValue(), equalTo(propertyValue));
        return;
      }
    }
    fail(String.format("configs did not contain property %s", propertyName));
  }

  public static void assertExists(List<Config> configs, String propertyName) {
    for (Config config : configs) {
      if (config.getName().equals(propertyName)) {
        return;
      }
    }
    fail(String.format("configs did not contain property %s", propertyName));
  }

  /**
   * <p>
   *   Captures the state of a list of {@link Config} objects.  Call this before the upgrader runs to capture the
   *   initial state, then call {@link UpgradeMoveWatcher#assertAllMoved} on the returned {@link UpgradeMoveWatcher}
   *   after the upgrader method runs
   * </p>
   * @param configs the list of initial (pre-upgrade) configs
   * @return an instance of the {@link UpgradeMoveWatcher} with the initial state captured
   */
  public static UpgradeMoveWatcher snapshot(List<Config> configs) {
    return new UpgradeMoveWatcher(configs);
  }

}
