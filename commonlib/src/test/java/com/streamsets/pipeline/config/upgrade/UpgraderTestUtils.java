/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.streamsets.pipeline.api.Config;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
      BiMap<String, String> newToOld = HashBiMap.create(oldToNewNames).inverse();

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
              StringUtils.join(newNamesSeen)
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
