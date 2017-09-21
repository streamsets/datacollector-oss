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

import com.streamsets.pipeline.api.credential.CredentialStore;

/**
 * Encapsulates the information and the implementation of a credential store.
 * <p/>
 * The returned store always executed in the context of the classloader that declared that implementation.
 */
public class CredentialStoreDefinition {
  private final String name;
  private final String label;
  private final String description;
  private final StageLibraryDefinition stageLibraryDefinition;
  private final Class<? extends CredentialStore> storeClass;

  public CredentialStoreDefinition(
      StageLibraryDefinition stageLibraryDefinition,
      Class<? extends CredentialStore> storeClass,
      String name,
      String label,
      String description
  ) {
    this.stageLibraryDefinition = stageLibraryDefinition;
    this.storeClass = storeClass;
    this.name = name;
    this.label = label;
    this.description = description;
  }

  public String getName() {
    return name;
  }

  public String getLabel() {
    return label;
  }

  public String getDescription() {
    return description;
  }

  public StageLibraryDefinition getStageLibraryDefinition() {
    return stageLibraryDefinition;
  }

  public Class<? extends CredentialStore> getStoreClass() {
    return storeClass;
  }

}
