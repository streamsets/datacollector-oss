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
package com.streamsets.pipeline.stage.origin.maprjson;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  MAPR_JSON_ORIGIN_01("Table name cannot be blank"),
  MAPR_JSON_ORIGIN_03("Failed to create documentStream. key '{}' "),
  MAPR_JSON_ORIGIN_04("Exception calling parse()"),
  MAPR_JSON_ORIGIN_05("Exception creating parserFactory()"),
  MAPR_JSON_ORIGIN_06("Exception getting reference to table '{}'"),
  MAPR_JSON_ORIGIN_07("Exception from Thread.sleep()"),
  MAPR_JSON_ORIGIN_08("Exception trying to restart documentStream. Key value '{}'"),
  MAPR_JSON_ORIGIN_09("Exception Closing table"),
  MAPR_JSON_ORIGIN_10("Exception fetching next document from input."),
  MAPR_JSON_ORIGIN_11("Failed to initialize dataFormatConfig"),
  MAPR_JSON_ORIGIN_12("Failed to fetch a sample document. "),
  MAPR_JSON_ORIGIN_13("Exception parsing binary key.  width '{}' value '{}' "),
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
