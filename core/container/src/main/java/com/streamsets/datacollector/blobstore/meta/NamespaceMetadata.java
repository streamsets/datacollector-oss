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

public class NamespaceMetadata {

  private final Map<String, ObjectMetadata> objects;

  public NamespaceMetadata() {
    this.objects = new HashMap<>();
  }

  @JsonCreator
  public NamespaceMetadata(
    @JsonProperty("objects") Map<String, ObjectMetadata> objects
  ) {
    this.objects = objects;
  }

  @JsonIgnore
  public ObjectMetadata getOrCreateObject(String object) {
    return objects.computeIfAbsent(object, ns -> new ObjectMetadata());
  }

  @JsonIgnore
  public void removeObject(String name) {
    objects.remove(name);
  }

  @JsonIgnore
  public ObjectMetadata getObject(String object) {
    return objects.get(object);
  }

  @JsonIgnore
  public Map<String, ObjectMetadata> getObjects() {
    return objects;
  }

  @JsonIgnore
  public boolean isEmpty() {
    return objects.isEmpty();
  }
}
