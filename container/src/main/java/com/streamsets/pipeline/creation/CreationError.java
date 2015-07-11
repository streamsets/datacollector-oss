/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.creation;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.validation.Issue;

@GenerateResourceBundle
public enum CreationError implements ErrorCode {

  CREATION_000("Failed to instantiate stage '{}' [ERROR]: {}"),
  CREATION_001("Failed to instantiate config bean '{}' [ERROR]: {}"),
  CREATION_002("Configuration definition missing '{}', there is a library/stage mismatch [ERROR]"),
  CREATION_003("Failed to access config bean [ERROR]: {}"),

  CREATION_004("Could not set default value to configuration [ERROR]: {}"),

  CREATION_005("Could not resolve implicit EL expression '{}': {}"),

  CREATION_006("Stage definition not found Library '{}' Stage '{}' Version '{}'"),

  CREATION_007("Stage definition Library '{}' Stage '{}' Version '{}' is for error stages only"),
  CREATION_008("Stage definition Library '{}' Stage '{}' Version '{}' is not for error stages"),

  CREATION_009("Missing error stage configuration"),

  CREATION_010("Configuration value '{}' is not a valid '{}' enum value: {}"),
  CREATION_011("Configuration value '{}' is not string, it is a '{}'"),
  CREATION_012("Configuration value '{}' is not character, it is a '{}'"),
  CREATION_013("Configuration value '{}' is not boolean, it is a '{}'"),
  CREATION_014("Configuration value '{}' is not number, it is a '{}'"),
  CREATION_015("Configuration value '{}' cannot be converted to '{}': {}"),

  CREATION_020("Configuration value is not a LIST"),
  CREATION_021("LIST configuration has a NULL value"),

  CREATION_030("Configuration value is not a MAP"),
  CREATION_031("MAP configuration value has invalid values type '{}'"),
  CREATION_032("MAP configuration has a NULL key"),
  CREATION_033("MAP configuration has a NULL value"),

  CREATION_040("ComplexField configuration is not a LIST, it is a '{}'"),
  CREATION_041("Failed to instantiate ComplexField bean '{}' [ERROR]: {}"),
  CREATION_042("ComplexField configuration value is invalid: {}"),

  CREATION_050("Configuration value cannot be NULL"),
  CREATION_051("Configuration type '{}' is invalid [ERROR]"),

  CREATION_060("Could not set configuration value '{}' [ERROR]: {}"),

  ;

  private final String msg;

  CreationError(String msg) {
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
