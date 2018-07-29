/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.blobstore.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class BlobStoreMetadata {

  private int version;
  private final Map<String, NamespaceMetadata> namespaces;

  public BlobStoreMetadata() {
    this.version = 1; // TODO: Some constant here?
    this.namespaces = new HashMap<>();
  }

  @JsonCreator
  public BlobStoreMetadata(
    @JsonProperty("version") int version,
    @JsonProperty("namespaces")  Map<String, NamespaceMetadata> namespaces
  ) {
    this.version = version;
    this.namespaces = namespaces;
  }

  public int getVersion() {
    return version;
  }

  public Map<String, NamespaceMetadata> getNamespaces() {
    return namespaces;
  }

  @JsonIgnore
  public NamespaceMetadata getOrCreateNamespace(String namespace) {
    return namespaces.computeIfAbsent(namespace, ns -> new NamespaceMetadata());
  }

  @JsonIgnore
  public NamespaceMetadata getNamespace(String namespace) {
    return namespaces.get(namespace);
  }

  @JsonIgnore
  public void removeNamespace(String namespace) {
    namespaces.remove(namespace);
  }
}
