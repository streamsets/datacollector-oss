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
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.config.PipelineFragmentDefinition;

import java.util.List;

public class PipelineFragmentDefinitionJson {

  private final PipelineFragmentDefinition pipelineFragmentDefinition;

  PipelineFragmentDefinitionJson(PipelineFragmentDefinition pipelineFragmentDefinition) {
    this.pipelineFragmentDefinition = pipelineFragmentDefinition;
  }

  public List<ConfigDefinitionJson> getConfigDefinitions() {
    return BeanHelper.wrapConfigDefinitions(pipelineFragmentDefinition.getConfigDefinitions());
  }

  public ConfigGroupDefinitionJson getConfigGroupDefinition() {
    return BeanHelper.wrapConfigGroupDefinition(pipelineFragmentDefinition.getConfigGroupDefinition());
  }

}
