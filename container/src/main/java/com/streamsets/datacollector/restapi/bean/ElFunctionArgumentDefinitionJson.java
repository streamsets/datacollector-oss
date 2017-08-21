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

import com.streamsets.datacollector.el.ElFunctionArgumentDefinition;
import com.streamsets.pipeline.api.impl.Utils;

public class ElFunctionArgumentDefinitionJson {

  private final ElFunctionArgumentDefinition elFunctionArgumentDefinition;

  public ElFunctionArgumentDefinitionJson(ElFunctionArgumentDefinition elFunctionArgumentDefinition) {
    Utils.checkNotNull(elFunctionArgumentDefinition, "elFunctionArgumentDefinition");
    this.elFunctionArgumentDefinition = elFunctionArgumentDefinition;
  }

  public String getName() {
    return elFunctionArgumentDefinition.getName();
  }

  public String getType() {
    return elFunctionArgumentDefinition.getType();
  }

}
