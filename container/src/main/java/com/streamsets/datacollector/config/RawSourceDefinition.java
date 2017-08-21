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

import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class RawSourceDefinition {

  private final String rawSourcePreviewerClass;
  private final String mimeType;
  List<ConfigDefinition> configDefinitions;

  public RawSourceDefinition(String rawSourcePreviewerClass, String mimeType,
                             List<ConfigDefinition> configDefinitions) {
    this.rawSourcePreviewerClass = rawSourcePreviewerClass;
    this.configDefinitions = configDefinitions;
    this.mimeType = mimeType;
  }

  public String getRawSourcePreviewerClass() {
    return rawSourcePreviewerClass;
  }

  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  public String getMimeType() {
    return mimeType;
  }

  @Override
  public String toString() {
    return Utils.format("RawSourceDefinition[rawSourcePreviewerClass='{}' mimeType='{}']", getRawSourcePreviewerClass(),
      getMimeType());
  }
}
