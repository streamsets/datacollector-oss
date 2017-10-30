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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  SPOOLDIR_00("Could not archive file '{}': {}"),
  SPOOLDIR_01("Failed to process file '{}' at position '{}': {}"),

  SPOOLDIR_02("Object in file '{}' at offset '{}' exceeds maximum length"),
  SPOOLDIR_04("File '{}' could not be fully processed, failed on '{}' offset: {}"),

  SPOOLDIR_05("Unsupported charset '{}'"),

  SPOOLDIR_06("Buffer Limit must be equal or greater than 64 KB"),

  SPOOLDIR_10("Unsupported data format '{}'"),
  SPOOLDIR_11("Directory path cannot be empty"),
  SPOOLDIR_12("Directory '{}' does not exist"),
  SPOOLDIR_13("Path '{}' is not a directory"),
  SPOOLDIR_14("Batch size cannot be less than 1"),
  SPOOLDIR_15("Batch wait time '{}' cannot be less than 1"),
  SPOOLDIR_16("Invalid GLOB file pattern '{}': {}"),
  SPOOLDIR_17("Max files in directory cannot be less than 1"),
  SPOOLDIR_18("First file to process '{}' does not match the file pattern '{}"),
  SPOOLDIR_19("Archive retention time cannot be less than 0"),
  SPOOLDIR_20("Max data object length cannot be less than 1"),
  SPOOLDIR_23("Invalid XML element name '{}'"),
  SPOOLDIR_24("Cannot create the parser factory: '{}'"),
  SPOOLDIR_25("Low level reader overrun limit '{} KB' must be at least '{} KB'"),
  SPOOLDIR_27("Custom Log Format field cannot be empty"),
  SPOOLDIR_28("Error parsing custom log format string {}, reason {}"),
  SPOOLDIR_29("Error parsing regex {}, reason {}"),
  SPOOLDIR_30("RegEx {} contains {} groups but the field Path to group mapping specifies group {}."),
  SPOOLDIR_31("Error parsing grok pattern {}, reason {}"),
  SPOOLDIR_32("File Pattern cannot be empty"),
  SPOOLDIR_33("Cannot Serialize Offset: {}"),
  SPOOLDIR_34("Cannot Deserialize Offset: {}"),
  SPOOLDIR_35("Spool Directory Runner Failed. Reason {}"),
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
