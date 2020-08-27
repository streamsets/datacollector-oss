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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class UpgraderUtils {

  private static final Logger LOG = LoggerFactory.getLogger(UpgraderUtils.class);

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

  public static int prependToAll(List<Config> configs, String prefix, String... exceptProperties) {
    final Set<String> except = new HashSet<>(Arrays.asList(exceptProperties));
    Map<String, String> nameMap = new HashMap<>();
    for (Config config : configs) {
      final String name = config.getName();
      if (except.contains(name)) {
        continue;
      }
      nameMap.put(name, prefix + name);
    }
    return moveAllTo(configs, nameMap);
  }

  /**
   * Inserts insertStr into each config name, when its prefix matches the afterPrefix argument, and the remaining
   * portion appears in the suffixes set. The use case is to move properties that are direct children of a config bean
   * to some new sub-bean.
   *
   * @param configs the configs to operate on
   * @param afterPrefix the prefix of properties to be modified, after which the insertStr will be inserted
   * @param suffixes the suffixes of properties to be modified, before which the insertStr will be inserted
   * @param insertStr the string to insert in between the prefix and matching suffix
   * @return the modified configs
   */
  public static int insertAfterPrefix(
      List<Config> configs,
      String afterPrefix,
      Set<String> suffixes,
      String insertStr
  ) {
    Map<String, String> nameMap = new HashMap<>();
    for (Config config : configs) {
      final String name = config.getName();
      if (StringUtils.startsWith(name, afterPrefix)) {
        final int index = afterPrefix.length();
        final String remaining = StringUtils.substring(name, index);
        if (suffixes.contains(remaining)) {
          nameMap.put(name, afterPrefix + insertStr + remaining);
        }
      }
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
        LOG.info(String.format(
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

  /**
   * <p>
   * Returns a {@link Config} object from the supplied list with the supplied name, if it exists.
   * </p>
   *
   * @param configs list of config objects (will not be modified)
   * @param name the config to return, based on name
   * @return the config object by the given name, if it exists, and null otherwise
   */
  public static Config getConfigWithName(List<Config> configs, String name) {
    for (Config config : configs) {
      if (config.getName().equals(name)) {
        return config;
      }
    }
    return null;
  }


  /**
   * <p>
   * Returns a {@link Config} object from the supplied list with the supplied name, if it exists.  If a non-null
   * Config is returned, the supplied list of {@code configs} will be modified such that it no longer contains the
   * returned value.
   * </p>
   *
   * @param configs list of config objects (will not be modified)
   * @param name the config to return, based on name
   * @return the config object by the given name, if it exists, and null otherwise
   */
  public static Config getAndRemoveConfigWithName(List<Config> configs, String name) {
    final Config config = getConfigWithName(configs, name);
    if (config != null) {
      configs.remove(config);
    }
    return config;
  }
}
