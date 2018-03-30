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
package com.streamsets.datacollector.definition;

import com.streamsets.pipeline.api.ErrorCode;

public enum ELDefinitionError implements ErrorCode {
  //ELDefinitionExtractor
  ELDEF_000("{} Class='{}' Method='{}', method must be public to be an EL function"),
  ELDEF_001("{} Class='{}' Method='{}', EL function name cannot be empty"),
  ELDEF_002("{} Class='{}' Function='{}', method must be static"),
  ELDEF_003("{} Class='{}' Method='{}', invalid name '{}'"),
  ELDEF_004("{} Class='{}' Method='{}', invalid prefix '{}'"),
  ELDEF_005("{} Class='{}' Method='{}', parameter at position '{}' has '@ElParam' annotation missing"),

  ELDEF_010("{} Class='{}' Field='{}', field must public to be an EL constant"),
  ELDEF_011("{} Class='{}' Field='{}', EL constant name cannot be empty"),
  ELDEF_012("{} Class='{}' Function='{}', invalid name '{}'"),
  ELDEF_013("{} Class='{}' Field='{}', invalid name '{}'"),
  ;

  private final String msg;

  ELDefinitionError(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }
}
