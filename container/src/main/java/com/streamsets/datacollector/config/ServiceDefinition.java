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
package com.streamsets.datacollector.config;

import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.service.Service;

import java.util.List;

/**
 * Encapsulates information provided in ServiceDef and information that is required for framework.
 */
public class ServiceDefinition implements PrivateClassLoaderDefinition {
  private final StageLibraryDefinition libraryDefinition;
  private final Class<? extends Service> klass;
  private final ClassLoader classLoader;
  private final Class provides;
  private final int version;
  private final String label;
  private final String description;
  private final ConfigGroupDefinition groupDefinition;
  private final List<ConfigDefinition> configDefinitions;
  private final boolean privateClassloader;
  private final StageUpgrader upgrader;

  public ServiceDefinition(
    StageLibraryDefinition libraryDefinition,
    Class<? extends Service> klass,
    Class provides,
    ClassLoader classLoader,
    int version,
    String label,
    String description,
    ConfigGroupDefinition groupDefinition,
    List<ConfigDefinition> configDefinitions,
    boolean privateClassloader,
    StageUpgrader upgrader
  ) {
    this.libraryDefinition = libraryDefinition;
    this.klass = klass;
    this.provides = provides;
    this.classLoader = classLoader;
    this.version = version;
    this.label = label;
    this.description = description;
    this.groupDefinition = groupDefinition;
    this.configDefinitions = configDefinitions;
    this.privateClassloader = privateClassloader;
    this.upgrader = upgrader;
  }

  public ServiceDefinition(
    ServiceDefinition def,
    ClassLoader classLoader
  ) {
    this.libraryDefinition = def.libraryDefinition;
    this.klass = def.klass;
    this.provides = def.provides;
    this.classLoader = classLoader;
    this.version = def.version;
    this.label = def.label;
    this.description = def.description;
    this.groupDefinition = def.groupDefinition;
    this.configDefinitions = def.configDefinitions;
    this.privateClassloader = def.privateClassloader;
    this.upgrader = def.upgrader;
  }

  public StageLibraryDefinition getLibraryDefinition() {
    return libraryDefinition;
  }

  public Class<? extends Service> getKlass() {
    return klass;
  }

  public String getClassName() {
    return klass.getName();
  }

  public Class getProvides() {
    return provides;
  }

  public ConfigGroupDefinition getGroupDefinition() {
    return groupDefinition;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public String getName() {
    return klass.getName();
  }

  @Override
  public ClassLoader getStageClassLoader() {
    return classLoader;
  }

  @Override
  public boolean isPrivateClassLoader() {
    return privateClassloader;
  }

  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  public String getLabel() {

    return label;
  }

  public String getDescription() {
    return description;
  }

  public StageUpgrader getUpgrader() {
    return upgrader;
  }

}
