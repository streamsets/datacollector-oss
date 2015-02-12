/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.streamsets.pipeline.api.ErrorCode;

public enum ValidationError implements ErrorCode {
  VALIDATION_0000("Unsupported pipeline schema '{}'"),

  VALIDATION_0001("The pipeline is empty"),
  VALIDATION_0002("Stages are not fully wired, cannot reach the following stages '{}'"),
  VALIDATION_0003("The first stage must be a Source"),
  VALIDATION_0004("This stage cannot be a Source"),
  VALIDATION_0005("Stage name already defined"),
  VALIDATION_0006("Stage definition does not exist, library '{}' name '{}' version '{}'"),
  VALIDATION_0007("Configuration value is required"),
  VALIDATION_0008("Invalid configuration"),
  VALIDATION_0009("Configuration should be a '{}'"),
  VALIDATION_0010("Output streams '{}' already defined by stage '{}'"),
  VALIDATION_0011("Stage has open output streams"),
    // Issues are augmented with an array of the open streams in the additionalInfo map property

  VALIDATION_0012("{} cannot have input streams '{}'"),
  VALIDATION_0013("{} cannot have output streams '{}'"),
  VALIDATION_0014("{} must have input streams"),
  VALIDATION_0015("Stage must have '{}' output stream(s) but has '{}'"),
  VALIDATION_0016("Invalid stage name, names can only contain the following characters '{}'"),
  VALIDATION_0017("Invalid input stream names '{}', streams can only contain the following characters '{}'"),
  VALIDATION_0018("Invalid output stream names '{}', streams can only contain the following characters '{}'"),

  VALIDATION_0019("Stream condition at index '{}' is not a Map"),
  VALIDATION_0020("Stream condition at index '{}' must have a '{}' entry"),
  VALIDATION_0021("Stream condition at index '{}' entry '{}' cannot be NULL"),
  VALIDATION_0022("Stream condition at index '{}' entry '{}' must be a String"),
  VALIDATION_0023("Stream condition at index '{}' entry '{}' cannot be empty"),

  VALIDATION_0024("Configuration of type Map expressed as List of key/value pairs cannot have NULL elements in the " +
                  "list, element at index '{}' is NULL"),
  VALIDATION_0025("Configuration of type Map expressed as List of key/value pairs must have the 'key' and 'value'" +
                  "entries in the List's Map elements, element at index '{}' is does not have those 2 entries"),
  VALIDATION_0026("Configuration of type Map expressed as List of key/value pairs must have Map entries in the List," +
                  "element at index '{}' has '{}'"),

  VALIDATION_0027("Rule Definition '{}' refers to a stream '{}' which is not found in the pipeline configuration."),
  VALIDATION_0028("Rule Definition '{}' refers to a stage '{}' which is not found in the pipeline configuration."),

  VALIDATION_0029("Configuration must be a String, is a '{}'"),
  VALIDATION_0030("The expression value '{}' must be within '${...}'"),

  VALIDATION_0031("Configuration '{}' should be a single character"),
  VALIDATION_0032("Stage must have at least one output stream"),

  //Rule Validation Errors
  VALIDATION_0040("Configuration '{}' must be set for Data Rule Definition."),
  VALIDATION_0041("Configuration 'SamplingPercentage' must have a value between 0 and 100."),
  VALIDATION_0042("Email alert is enabled but no email is specified."),
  VALIDATION_0043("The value specified for configuration 'threshold' is not a number."),
  VALIDATION_0044("Configuration 'threshold' must have a value between 0 and 100."),
  VALIDATION_0045("Condition '{}' set for Data Rule Definition is not valid."),
  VALIDATION_0046("Condition must be of the form '${value()<op><number>}'."),

  VALIDATION_0050("Configuration '{}' must be set for Metric Alert Definition."),


  VALIDATION_0060("Bad records handling must be specified"),
  VALIDATION_0061("Configuration 'Directory' must be set for Bad Record handling."),

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
