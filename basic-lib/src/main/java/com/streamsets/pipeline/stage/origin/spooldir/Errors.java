/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  SPOOLDIR_00("Could not archive file '{}': {}"),
  SPOOLDIR_01("Error while processing file '{}' at position '{}': {}"),

  SPOOLDIR_02("Object in file '{}' at offset '{}' exceeds maximum length"),
  SPOOLDIR_04("File '{}' could not be fully processed, failed on '{}' offset: {}"),

  SPOOLDIR_05("Unsupported charset '{}'"),

  SPOOLDIR_10("Unsupported data format '{}'"),
  SPOOLDIR_11("Directory path cannot be empty"),
  SPOOLDIR_12("Directory '{}' does not exist"),
  SPOOLDIR_13("Path '{}' is not a directory"),
  SPOOLDIR_14("Batch size cannot be less than 1"),
  SPOOLDIR_15("Batch wait time '{}' cannot be less than 1"),
  SPOOLDIR_16("Invalid GLOB file pattern '{}': {}"),
  SPOOLDIR_17("Max files in directory cannot be less than 1"),
  SPOOLDIR_18("First file to process '{}' does not match the file pattern '{}"),
  SPOOLDIR_19("Archive retention time cannot be less than 1"),
  SPOOLDIR_20("Max data object length cannot be less than 1"),
  SPOOLDIR_23("Invalid XML element name '{}'"),
  SPOOLDIR_24("Cannot create the parser factory: '{}'"),
  SPOOLDIR_25("Low level reader overrun limit '{} KB' must be at least '{} KB'"),
  SPOOLDIR_26("XML delimiter element cannot be empty"),
  SPOOLDIR_27("Custom Log Format field cannot be empty"),
  SPOOLDIR_28("Error parsing custom log format string {}, reason {}"),
  SPOOLDIR_29("Error parsing regex {}, reason {}"),
  SPOOLDIR_30("RegEx {} contains {} groups but the field Path to group mapping specifies group {}."),
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
