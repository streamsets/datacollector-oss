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
package com.streamsets.pipeline.lib.generator.xml;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  XML_GENERATOR_00("Record root field must be Map or ListMap instead of '{}'"),
  XML_GENERATOR_01("Record Field '{}' of type Field Ref is unsupported for json serialization"),
  XML_GENERATOR_02("Record generated XML does not conform to specified schemas(s): {}"),
  XML_GENERATOR_03("Record does not have a root field"),
  XML_GENERATOR_04("Record root field must have 1 element"),
  XML_GENERATOR_05("Record root field cannot be a List"),
  XML_GENERATOR_06("Record didn't generate any XML"),
  XML_GENERATOR_07("Undefined namespace for '{}' attribute prefix"),
  XML_GENERATOR_08("Undefined namespace for '{}' element prefix"),
  ;

  private final String msg;

  Errors(String msg) {
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
