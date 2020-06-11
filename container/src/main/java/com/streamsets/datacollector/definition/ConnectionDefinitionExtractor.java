/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.definition;

import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ConfigGroupDefinition;
import com.streamsets.datacollector.config.ConnectionDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang3.ClassUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public abstract class ConnectionDefinitionExtractor {

  private static final ConnectionDefinitionExtractor EXTRACTOR = new ConnectionDefinitionExtractor() {};

  public static ConnectionDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public static List<String> getGroups(Class klass) {
    Set<String> set = new LinkedHashSet<>();
    addGroupsToList(klass, set);
    List<Class<?>> allSuperclasses = ClassUtils.getAllSuperclasses(klass);
    for(Class<?> superClass : allSuperclasses) {
      if(!superClass.isInterface() && superClass.isAnnotationPresent(ConfigGroups.class)) {
        addGroupsToList(superClass, set);
      }
    }
    if(set.isEmpty()) {
      set.add(""); // the default empty group
    }

    return new ArrayList<>(set);
  }

  @SuppressWarnings("unchecked")
  private static void addGroupsToList(Class<?> klass, Set<String> set) {
    ConfigGroups groups = klass.getAnnotation(ConfigGroups.class);
    if (groups != null) {
      Class<? extends Enum> groupKlass = (Class<? extends Enum>) groups.value();
      for (Enum e : groupKlass.getEnumConstants()) {
        set.add(e.name());
      }
    }
  }

  public ConnectionDefinition extract(StageLibraryDefinition libraryDef, Class<?> klass) {
    ConnectionDef conDef = klass.getAnnotation(ConnectionDef.class);
    Utils.formatL("Connection Definition: Connection='{}'", conDef.label());
    try {
      int version = conDef.version();
      String label = conDef.label();
      String description = conDef.description();
      String type = conDef.type();
      List<ConfigDefinition> configDefinitions = extractConfigDefinitions(klass);
      String contextMsg = Utils.format("Connection='{}'", klass.getSimpleName());
      ConfigGroupDefinition configGroupDefinition = ConfigGroupExtractor.get().extract(klass, contextMsg);
      String yamlUpgrader = conDef.upgraderDef();
      String verifier = conDef.verifier().getCanonicalName();

      return new ConnectionDefinition(
              conDef,
              libraryDef,
              version,
              label,
              description,
              type,
              configDefinitions,
              configGroupDefinition,
              yamlUpgrader,
              verifier
      );
    } catch (Exception e) {
      throw new IllegalStateException("Exception while extracting connection definition for " + conDef.label(), e);
    }
  }

  private List<ConfigDefinition> extractConfigDefinitions(Class<?> klass) {
    List<String> stageGroups = getGroups(klass);
    return ConfigDefinitionExtractor.get().extract(klass, stageGroups, "Connection Configuration");
  }
}