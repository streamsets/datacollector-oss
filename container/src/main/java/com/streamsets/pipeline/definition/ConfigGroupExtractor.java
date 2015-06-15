/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigGroupDefinition;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ConfigGroupExtractor {

  private static final ConfigGroupExtractor EXTRACTOR = new ConfigGroupExtractor() {};

  public static ConfigGroupExtractor get() {
    return EXTRACTOR;
  }

  public List<ErrorMessage> validate(Class<? extends Stage> klass, Object contextMsg) {
    List<ErrorMessage> errors = new ArrayList<>();
    List<ConfigGroups> allConfigGroups = getAllConfigGroups(klass);
    Set<String> allGroupNames = new HashSet<>();
    if (!allConfigGroups.isEmpty()) {
      for (ConfigGroups configGroups : allConfigGroups) {
        Class<? extends Label> gKlass = configGroups.value();
        if (!gKlass.isEnum()) {
          errors.add(new ErrorMessage(DefinitionError.DEF_100, contextMsg, gKlass.getSimpleName()));
        } else {
          for (Label label : gKlass.getEnumConstants()) {
            String groupName = label.toString();
            if (allGroupNames.contains(groupName)) {
              errors.add(new ErrorMessage(DefinitionError.DEF_101, contextMsg, groupName));
            }
            allGroupNames.add(groupName);
          }
        }
      }
    }
    return errors;
  }

  public ConfigGroupDefinition extract(Class<? extends Stage> klass, Object contextMsg) {
    List<ErrorMessage> errors = validate(klass, contextMsg);
    if (errors.isEmpty()) {
      List<ConfigGroups> allConfigGroups = getAllConfigGroups(klass);
      Set<String> allGroupNames = new HashSet<>();
      Map<String, List<String>> classNameToGroupsMap = new HashMap<>();
      List<Map<String, String>> groupNameToLabelMapList = new ArrayList<>();
      if (!allConfigGroups.isEmpty()) {
        for (ConfigGroups configGroups : allConfigGroups) {
          Class<? extends Label> gKlass = configGroups.value();
          List<String> groupNames = new ArrayList<>();
          classNameToGroupsMap.put(gKlass.getName(), groupNames);
          for (Label label : gKlass.getEnumConstants()) {
            String groupName = label.toString();
            Map<String, String> groupNameToLabelMap = new LinkedHashMap<>();
            allGroupNames.add(groupName);
            groupNames.add(groupName);
            groupNameToLabelMap.put("name", groupName);
            groupNameToLabelMap.put("label", label.getLabel());
            groupNameToLabelMapList.add(groupNameToLabelMap);
          }
        }
      }
      return new ConfigGroupDefinition(allGroupNames, classNameToGroupsMap, groupNameToLabelMapList);
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid ConfigGroup definition: {}", errors));
    }
  }

  private List<ConfigGroups> getAllConfigGroups(Class klass) {
    List<ConfigGroups> groups;
    if (klass == Object.class) {
      groups = new ArrayList<>();
    } else {
      groups = getAllConfigGroups(klass.getSuperclass());
      Annotation annotation = klass.getAnnotation(ConfigGroups.class);
      if (annotation != null) {
        groups.add((ConfigGroups)annotation);
      }
    }
    return groups;
  }
}
