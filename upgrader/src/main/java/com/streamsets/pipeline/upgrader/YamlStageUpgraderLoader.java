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
package com.streamsets.pipeline.upgrader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Config;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class YamlStageUpgraderLoader {

  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  private final String stageName;
  private final URL resource;

  public YamlStageUpgraderLoader(String stageName, URL resource) {
    this.stageName = stageName;
    this.resource = resource;
  }

  public YamlStageUpgrader get() {
    try (InputStream is = resource.openStream()) {
      Map yaml = OBJECT_MAPPER.readValue(is, Map.class);
      Integer upgraderVersion = (Integer) yaml.get("upgraderVersion");
      if (upgraderVersion == null || upgraderVersion != 1) {
        throw new StageException(Errors.YAML_UPGRADER_10, upgraderVersion);
      }
      YamlStageUpgrader upgrader = new YamlStageUpgrader();
      upgrader.setStageName(stageName);
      upgrader.setUpgraderVersion(upgraderVersion);
      upgrader.setToVersionMap(parseToVersions((List) yaml.get("upgrades")));
      return upgrader;
    } catch (Exception ex) {
      throw new StageException(Errors.YAML_UPGRADER_07, resource, ex);
    }
  }

  Map<Integer, UpgradeToVersion> parseToVersions(List list) {
    Map<Integer, UpgradeToVersion> toVersions = new LinkedHashMap<>();
    if (list != null) {
      for (Map map : (List<Map>) list) {
        if (map.containsKey("toVersion")) {
          Integer toVersion = (Integer) map.get("toVersion");
          List actions = (List) map.get("actions");
          toVersions.put(toVersion, parseConfigActions(toVersion, actions));
        }
      }
    }
    return toVersions;
  }

  UpgradeToVersion parseConfigActions(Integer version, List list) {
    UpgradeToVersion toVersion = new UpgradeToVersion();
    List<UpgraderAction<?, List<Config>>> upgraderActions = new ArrayList<>();
    for (Map action : (List<Map>) list) {
      upgraderActions.add(parseConfigAction(UpgraderAction.CONFIGS_WRAPPER, version, action));
    }
    toVersion.setActions(upgraderActions);
    return toVersion;
  }

  <T> UpgraderAction<?, T> parseConfigAction(
      Function<?, UpgraderAction.ConfigsAdapter> wrapper,
      Integer toVersion,
      Map map
  ) {
    UpgraderAction action;
    if (map.containsKey("setConfig")) {
      action = parseSetConfigAction(wrapper, map);
    } else if (map.containsKey("renameConfig")) {
      action = parseRenameConfigAction(wrapper, map);
    } else if (map.containsKey("removeConfigs")) {
      action = parseRemoveConfigs(wrapper, map);
    } else if (map.containsKey("replaceConfig")) {
      action = parseReplaceConfig(wrapper, map);
    } else if (map.containsKey("iterateListConfig")) {
      action = parseIterateListConfig(wrapper, toVersion, map);
    } else if (map.containsKey("configStringMapPut")) {
      action = parseConfigStringMapPutConfig(wrapper, map);
    } else if (map.containsKey("configStringMapRemove")) {
      action = parseConfigStringMapRemoveConfig(wrapper, map);
    } else if (map.containsKey("configStringListAdd")) {
      action = parseConfigStringListAddConfig(wrapper, map);
    } else if (map.containsKey("configStringListRemove")) {
      action = parseConfigStringListRemoveConfig(wrapper, map);
    } else if (map.containsKey("registerService")) {
      action = parseRegisterService(wrapper, map);
    } else if (map.containsKey("setConfigFromStringMap")) {
      action = parseSetConfigFromStringMapAction(wrapper, map);
    } else if (map.containsKey("setConfigFromConfigList")) {
      action = parseSetConfigFromConfigListAction(wrapper, map);
    } else if (map.containsKey("setConfigFromJoinedConfigList")) {
      action = parseSetConfigFromJoinedConfigListAction(wrapper, map);
    } else {
      throw new StageException(Errors.YAML_UPGRADER_08, toVersion, stageName, resource);
    }
    return action;
  }

  private RegisterServiceAction<?> parseRegisterService(Function<?, UpgraderAction.ConfigsAdapter> wrapper, Map map) {
    Map registerService = (Map) map.get("registerService");
    String service = (String)registerService.get("service");
    String existingConfigPattern = (String) registerService.get("existingConfigsPattern");
    RegisterServiceAction<?> serviceAction = new RegisterServiceAction<>(wrapper);
    serviceAction.setService(service);
    serviceAction.setExistingConfigsPattern(existingConfigPattern);
    return serviceAction;
  }

  SetConfigUpgraderAction parseSetConfigAction(
      Function<?, UpgraderAction.ConfigsAdapter> wrapper,
      Map<String, Object> map
  ) {
    map = (Map) map.get("setConfig");
    SetConfigUpgraderAction action = new SetConfigUpgraderAction<>(wrapper);
    action.setName((String) map.get("name"));
    action.setValue(map.get("value"));
    action.setElseName((String) map.get("elseName"));
    action.setElseValue(map.get("elseValue"));
    action.setLookForName((String) map.get("lookForName"));
    action.setIfValueMatches(map.get("ifValueMatches"));
    return action;
  }

  RenameConfigUpgraderAction parseRenameConfigAction(
      Function<?, UpgraderAction.ConfigsAdapter> wrapper,
      Map<String, Object> map
  ) {
    map = (Map) map.get("renameConfig");
    RenameConfigUpgraderAction action = new RenameConfigUpgraderAction<>(wrapper);
    action.setOldNamePattern((String) map.get("oldNamePattern"));
    action.setNewNamePattern((String) map.get("newNamePattern"));
    return action;
  }

  RemoveConfigUpgraderAction parseRemoveConfigs(
      Function<?, UpgraderAction.ConfigsAdapter> wrapper,
      Map<String, Object> map
  ) {
    map = (Map) map.get("removeConfigs");
    RemoveConfigUpgraderAction action = new RemoveConfigUpgraderAction<>(wrapper);
    action.setNamePattern((String) map.get("namePattern"));
    return action;
  }

  ReplaceConfigUpgraderAction parseReplaceConfig(
      Function<?, UpgraderAction.ConfigsAdapter> wrapper,
      Map<String, Object> map
  ) {
    map = (Map) map.get("replaceConfig");
    ReplaceConfigUpgraderAction action =
        new ReplaceConfigUpgraderAction<>(wrapper);
    action.setName((String) map.get("name"));
    action.setNewValue(map.get("newValue"));
    action.setElseNewValue(map.get("elseNewValue"));
    if (map.containsKey("ifOldValueMatches")) {
      action.setIfOldValueMatches(map.get("ifOldValueMatches"));
    }
    action.setTokenForOldValue((String) map.get("tokenForOldValue"));
    if (map.containsKey("newValueFromMatchIndex")) {
      action.setNewValueFromMatchIndex(map.get("newValueFromMatchIndex"));
    }
    return action;
  }

  StringMapConfigPutUpgraderAction parseConfigStringMapPutConfig(
      Function<?, UpgraderAction.ConfigsAdapter> wrapper,
      Map<String, Object> map
  ) {
    map = (Map) map.get("configStringMapPut");
    StringMapConfigPutUpgraderAction action = new StringMapConfigPutUpgraderAction<>(wrapper);
    action.setName((String) map.get("name"));
    action.setKey((String) map.get("key"));
    action.setValue(map.get("value"));
    action.setLookForName((String) map.get("lookForName"));
    action.setIfValueMatches(map.get("ifValueMatches"));
    return action;
  }

  StringMapConfigRemoveUpgraderAction parseConfigStringMapRemoveConfig(
      Function<?, UpgraderAction.ConfigsAdapter> wrapper,
      Map<String, Object> map
  ) {
    map = (Map) map.get("configStringMapRemove");
    StringMapConfigRemoveUpgraderAction action = new StringMapConfigRemoveUpgraderAction<>(wrapper);
    action.setName((String) map.get("name"));
    action.setKey((String) map.get("key"));
    action.setValue(map.get("value"));
    action.setLookForName((String) map.get("lookForName"));
    action.setIfValueMatches(map.get("ifValueMatches"));
    return action;
  }

  StringListConfigAddUpgraderAction parseConfigStringListAddConfig(
      Function<?, UpgraderAction.ConfigsAdapter> wrapper,
      Map<String, Object> map
  ) {
    map = (Map) map.get("configStringListAdd");
    StringListConfigAddUpgraderAction action = new StringListConfigAddUpgraderAction<>(wrapper);
    action.setName((String) map.get("name"));
    action.setValue((String) map.get("value"));
    action.setLookForName((String) map.get("lookForName"));
    action.setIfValueMatches(map.get("ifValueMatches"));
    return action;
  }

  StringListConfigRemoveUpgraderAction parseConfigStringListRemoveConfig(
      Function<?, UpgraderAction.ConfigsAdapter> wrapper,
      Map<String, Object> map
  ) {
    map = (Map) map.get("configStringListRemove");
    StringListConfigRemoveUpgraderAction action = new StringListConfigRemoveUpgraderAction<>(wrapper);
    action.setName((String) map.get("name"));
    action.setValue((String) map.get("value"));
    action.setLookForName((String) map.get("lookForName"));
    action.setIfValueMatches(map.get("ifValueMatches"));
    return action;
  }

  IterateConfigListUpgraderAction parseIterateListConfig(
      Function<?, UpgraderAction.ConfigsAdapter> wrapper,
      int toVersion,
      Map<String, Object> map
  ) {
    if (wrapper != UpgraderAction.CONFIGS_WRAPPER) {
      throw new StageException(Errors.YAML_UPGRADER_11, toVersion, stageName, resource);
    }
    map = (Map) map.get("iterateListConfig");
    String name = (String) map.get("name");
    IterateConfigListUpgraderAction iterateAction = new IterateConfigListUpgraderAction();
    iterateAction.setName(name);
    List actions = (List) map.get("actions");
    List<UpgraderAction<?, Map<String, Object>>> upgraderActions = new ArrayList<>();
    for (Map action : (List<Map>) actions) {
      upgraderActions.add(parseConfigAction(UpgraderAction.MAP_WRAPPER, toVersion, action));
    }
    iterateAction.setActions(upgraderActions);
    return iterateAction;
  }

  SetConfigFromStringMapUpgraderAction parseSetConfigFromStringMapAction(
      Function<?, UpgraderAction.ConfigsAdapter> wrapper,
      Map<String, Object> map
  ) {
    map = (Map) map.get("setConfigFromStringMap");
    String name = (String) map.get("name");
    String mapName = (String) map.get("mapName");
    String key = (String) map.get("key");
    SetConfigFromStringMapUpgraderAction action = new SetConfigFromStringMapUpgraderAction(wrapper);
    action.setName(name);
    action.setMapName(mapName);
    action.setKey(key);
    return action;
  }

  SetConfigFromConfigListAction parseSetConfigFromConfigListAction(
      Function<?, UpgraderAction.ConfigsAdapter> wrapper,
      Map<String, Object> map
  ) {
    map = (Map) map.get("setConfigFromConfigList");
    String name = (String) map.get("name");
    String listName = (String) map.get("listName");
    String elementName = (String) map.get("elementName");
    boolean deleteFieldInList = (Boolean) map.get("deleteFieldInList");
    boolean elementMustExist = (Boolean) map.get("elementMustExist");
    SetConfigFromConfigListAction action = new SetConfigFromConfigListAction(wrapper);
    action.setName(name);
    action.setListName(listName);
    action.setElementName(elementName);
    action.setDeleteFieldInList(deleteFieldInList);
    action.setElementMustExist(elementMustExist);
    return action;
  }

  SetConfigFromJoinedConfigListAction parseSetConfigFromJoinedConfigListAction(
      Function<?, UpgraderAction.ConfigsAdapter> wrapper,
      Map<String, Object> map
  ) {
    map = (Map) map.get("setConfigFromJoinedConfigList");
    String name = (String) map.get("name");
    String listName = (String) map.get("listName");
    String delimiter = (String) map.get("delimiter");
    SetConfigFromJoinedConfigListAction action = new SetConfigFromJoinedConfigListAction(wrapper);
    action.setName(name);
    action.setListName(listName);
    action.setDelimiter(delimiter);
    return action;
  }

}
