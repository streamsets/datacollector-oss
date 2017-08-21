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
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.bundles.BundleContentGeneratorDefinition;

public class SupportBundleContentDefinitionJson {

  BundleContentGeneratorDefinition definition;

  public SupportBundleContentDefinitionJson(BundleContentGeneratorDefinition definition) {
    this.definition = definition;
  }

  public String getKlass() {
    return definition.getKlass().getName();
  }

  public String getName() {
    return definition.getName();
  }

  public String getId() {
    return definition.getId();
  }

  public String getDescription() {
    return definition.getDescription();
  }

  public int getVersion() {
    return definition.getVersion();
  }

  public boolean isEnabledByDefault() {
    return definition.isEnabledByDefault();
  }
}
