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
package com.streamsets.datacollector.usagestats;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use= JsonTypeInfo.Id.CLASS, property="class")
public interface StatsBeanExtension {
  /**
   * Return a version, like 1.0 or 1.2.1, that represents the version of the JSON for this extension. Whenever a
   * backwards incompatible change is done (behavioral or structural), this version must be incremented.
   * @return version for this extension's JSON
   */
  String getVersion();

  /**
   * Called by deserialization code. If this differs from the current version, must perform upgrades as needed, though
   * there is no guarantee of the order of set calls, so you should use a @JsonConstructor to handle upgrades.
   * @param version version from serialized object
   */
  void setVersion(String version);
}
