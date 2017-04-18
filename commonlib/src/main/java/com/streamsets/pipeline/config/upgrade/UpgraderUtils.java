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

import com.streamsets.pipeline.api.Config;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class UpgraderUtils {

  private static final Logger LOGGER = Logger.getLogger(UpgraderUtils.class);

  /**
   * <p>
   * Move a series of config values from old names to new names (one to one correspondence).  Config values will be
   * preserved.
   * </p>
   *
   * @param configs list of config objects (will be modified)
   * @param names mapping of old to new names, in the form of [oldName1, newName1, oldName2, newName2, ...]
   * @return
   */
  public static int moveAllTo(List<Config> configs, String... names) {
    Map<String, String> nameMap = new HashMap<>();
    if (names.length % 2 == 1) {
      throw new IllegalArgumentException("names was of uneven length");
    }
    for (int i=0; i<names.length; ) {
      nameMap.put(names[i], names[i+1]);
      i+=2;
    }
    return moveAllTo(configs, nameMap);
  }

  /**
   * <p>
   * Move a series of config values from old names to new names (one to one correspondence).  Config values will be
   * preserved.
   * </p>
   *
   * @param configs list of config objects (will be modified)
   * @param oldToNewNames a map of old to new config names
   * @return the number of config objects moved from old to new names
   */
  public static int moveAllTo(List<Config> configs, Map<String, String> oldToNewNames) {
    List<Config> configsToAdd = new ArrayList<>();
    List<Config> configsToRemove = new ArrayList<>();

    int numMoved = 0;
    for (Config config : configs) {
      final String oldName = config.getName();
      if (oldToNewNames.containsKey(oldName)) {
        configsToRemove.add(config);
        final Object value = config.getValue();
        final String newName = oldToNewNames.get(oldName);
        configsToAdd.add(new Config(newName, value));
        LOGGER.info(String.format(
            "Moving config value %s from old name %s to new name %s",
            value,
            oldName,
            newName
        ));
        numMoved++;
      }
    }
    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
    return numMoved;
  }
}
