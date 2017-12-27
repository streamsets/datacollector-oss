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
package com.streamsets.pipeline.sdk;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ProtoRunner {

  protected Status status;

  enum Status { CREATED, INITIALIZED, DESTROYED}

  static {
    RuntimeInfo runtimeInfo = new StandaloneRuntimeInfo(
        RuntimeModule.SDC_PROPERTY_PREFIX,
        new MetricRegistry(),
        Collections.singletonList(ProtoRunner.class.getClassLoader())
    );
    try {
      RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  void ensureStatus(Status status) {
    Utils.checkState(this.status == status, Utils.format("Current status '{}', expected '{}'", this.status, status));
  }

  // Configuration

  private boolean isConfigurationActive(ConfigDef configDef, Map<String, Object> configuration) {
    String dependsOn = configDef.dependsOn();
    if (!dependsOn.isEmpty()) {
      Object dependsOnValue = configuration.get(dependsOn);
      if (dependsOnValue != null) {
        String valueStr = dependsOnValue.toString();
        for (String trigger : configDef.triggeredByValue()) {
          if (valueStr.equals(trigger)) {
            return true;
          }
        }
        return false;
      }
      return false;
    }
    return true;
  }

  private Set<String> getStageConfigurationFields(Class<?> klass) throws Exception {
    Set<String> names = new HashSet<>();
    for (Field field : klass.getFields()) {
      if (field.isAnnotationPresent(ConfigDef.class)) {
        names.add(field.getName());
      }
    }
    return names;
  }

  private Set<String> filterNonActiveConfigurationsFromMissing(Object stage, Map<String, Object> configuration, Set<String> missingConfigs) {
    missingConfigs = new HashSet<>(missingConfigs);
    Iterator<String> it = missingConfigs.iterator();
    while (it.hasNext()) {
      String name = it.next();
      try {
        Field field = stage.getClass().getField(name);
        ConfigDef annotation = field.getAnnotation(ConfigDef.class);
        if (!annotation.required() || !isConfigurationActive(annotation, configuration)) {
          it.remove();
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return missingConfigs;
  }

  @SuppressWarnings("unchecked")
  protected void configureObject(Object stage, Map<String, Object> configuration) {
    try {
      Set<String> fields = getStageConfigurationFields(stage.getClass());
      Set<String> configs = configuration.keySet();
      if (!fields.equals(configs)) {
        Set<String> missingConfigs = Sets.difference(fields, configs);
        Set<String> extraConfigs = Sets.difference(configs, fields);

        missingConfigs = filterNonActiveConfigurationsFromMissing(stage, configuration, missingConfigs);
        if (missingConfigs.size() + extraConfigs.size() > 0) { //x
          throw new RuntimeException(Utils.format(
              "Invalid stage configuration for '{}', Missing configurations '{}' and invalid configurations '{}'",
              stage.getClass().getName(), missingConfigs, extraConfigs));
        }
      }
      for (Field field : stage.getClass().getFields()) {
        if (field.isAnnotationPresent(ConfigDef.class)) {
          ConfigDef configDef = field.getAnnotation(ConfigDef.class);
          if (isConfigurationActive(configDef, configuration)) {
            if ( configDef.type() != ConfigDef.Type.MAP) {
              field.set(stage, configuration.get(field.getName()));
            } else {
              //we need to handle special case of List of Map elements with key/value entries
              Object value = configuration.get(field.getName());
              if (value != null && value instanceof List) {
                Map map = new HashMap();
                for (Map element : (List<Map>) value) {
                  if (!element.containsKey("key") || !element.containsKey("value")) {
                    throw new RuntimeException(Utils.format("Invalid stage configuration for '{}' Map as list must have" +
                                                            " a List of Maps all with 'key' and 'value' entries",
                                                            field.getName()));
                  }
                  String k = (String) element.get("key");
                  String v = (String) element.get("value");
                  map.put(k, v);
                }
                value = map;
              }
              field.set(stage, value);
            }
          }
        }
      }
    } catch (Exception ex) {
      if (ex instanceof RuntimeException) {
        throw (RuntimeException) ex;
      }
      throw new RuntimeException(ex);
    }
  }
}
