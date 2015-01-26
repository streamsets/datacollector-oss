/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.streamsets.pipeline.api.ErrorCode;

public enum ValidationError implements ErrorCode {

  VALIDATION_0001("The pipeline is empty"),
  VALIDATION_0002("Stages are not fully wired, cannot reach the following stages '{}'"),
  VALIDATION_0003("The first stage must be a Source"),
  VALIDATION_0004("This stage cannot be a Source"),
  VALIDATION_0005("Instance name already defined"),
  VALIDATION_0006("Stage definition does not exist, library '{}' name '{}' version '{}'"),
  VALIDATION_0007("Configuration value is required"),
  VALIDATION_0008("Invalid configuration"),
  VALIDATION_0009("Configuration should be a '{}'"),
  VALIDATION_0010("Output lanes '{}' already defined by stage instance '{}'"),
  VALIDATION_0011("Instance has open lanes '{}'"),
  VALIDATION_0012("{} cannot have input lanes '{}'"),
  VALIDATION_0013("{} cannot have output lanes '{}'"),
  VALIDATION_0014("{} must have input lanes"),
  VALIDATION_0015("{} must have output lanes"),
  VALIDATION_0016("Invalid instance name, names can only contain the following characters '{}'"),
  VALIDATION_0017("Invalid input lane names '{}', lanes can only contain the following characters '{}'"),
  VALIDATION_0018("Invalid output lane names '{}', lanes can only contain the following characters '{}'"),

  VALIDATION_0019("Lane predicate at index '{}' is not a Map"),
  VALIDATION_0020("Lane predicate at index '{}' must have a '{}' entry"),
  VALIDATION_0021("Lane predicate at index '{}' entry '{}' cannot be NULL"),
  VALIDATION_0022("Lane predicate at index '{}' entry '{}' must be a String"),
  VALIDATION_0023("Lane predicate at index '{}' entry '{}' cannot be empty"),

  VALIDATION_0024("Configuration of type Map expressed as List of key/value pairs cannot have NULL elements in the " +
                  "list, element at index '{}' is NULL"),
  VALIDATION_0025("Configuration of type Map expressed as List of key/value pairs must have the 'key' and 'value'" +
                  "entries in the List's Map elements, element at index '{}' is does not have those 2 entries"),
  VALIDATION_0026("Configuration of type Map expressed as List of key/value pairs must have Map entries in the List," +
                  "element at index '{}' has '{}'"),

  VALIDATION_0027("Rule Definition '{}' refers to a lane '{}' which is not found in the pipeline configuration."),
  VALIDATION_0028("Rule Definition '{}' refers to a instance '{}' which is not found in the pipeline configuration."),

  VALIDATION_0029("Configuration must be a String, is a '{}'"),
  VALIDATION_0030("The expression value '{}' must be within '${...}'"),

  VALIDATION_0031("Configuration '{}' should be a single character"),

  ;

  private final String msg;

  ValidationError(String msg) {
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
