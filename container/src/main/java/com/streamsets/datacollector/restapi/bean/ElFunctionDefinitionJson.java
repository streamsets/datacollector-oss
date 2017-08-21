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

import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class ElFunctionDefinitionJson {

  private final ElFunctionDefinition elFunctionDefinition;

  public ElFunctionDefinitionJson(ElFunctionDefinition elFunctionDefinition) {
    Utils.checkNotNull(elFunctionDefinition, "elFunctionDefinition");
    this.elFunctionDefinition = elFunctionDefinition;
  }

  public String getName() {
    return elFunctionDefinition.getName();
  }

  public String getDescription() {
    return elFunctionDefinition.getDescription();
  }

  public String getGroup() {
    return elFunctionDefinition.getGroup();
  }

  public String getReturnType() {
    return elFunctionDefinition.getReturnType();
  }

  public List<ElFunctionArgumentDefinitionJson> getElFunctionArgumentDefinition() {
    return BeanHelper.wrapElFunctionArgumentDefinitions(elFunctionDefinition.getElFunctionArgumentDefinition());
  }

}
