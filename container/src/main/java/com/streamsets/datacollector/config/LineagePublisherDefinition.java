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

import com.streamsets.pipeline.api.lineage.LineagePublisher;

/**
 * Encapsulates information provided in LineagePublisherDef and information that is required
 * for framework.
 */
public class LineagePublisherDefinition {
  private final StageLibraryDefinition libraryDefinition;
  private final ClassLoader classLoader;
  private final Class<? extends LineagePublisher> klass;
  private final String name;
  private final String label;
  private final String description;

  public String getLabel() {
    return label;
  }

  public String getDescription() {
    return description;
  }

  public LineagePublisherDefinition(
    StageLibraryDefinition libraryDefinition,
    ClassLoader classLoader,
    Class<? extends LineagePublisher> klass,
    String name,
    String label,
    String description
  ) {
    this.libraryDefinition = libraryDefinition;
    this.classLoader = classLoader;
    this.klass = klass;
    this.name = name;
    this.label = label;
    this.description = description;
  }

  public StageLibraryDefinition getLibraryDefinition() {
    return libraryDefinition;
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }

  public Class<? extends LineagePublisher> getKlass() {
    return klass;
  }

  public String getName() {
    return name;
  }

}
