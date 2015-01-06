/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.ErrorCode;

public enum StageLibError implements ErrorCode {

  // LogTailSource
  LIB_0001("Insufficient permissions to read the log file '{}'"),

  // Selector
  LIB_0010("Selector must have at least 1 output lane"),
  LIB_0011("There are more output lanes '{}' than lane-predicates '{}'"),
  LIB_0012("The Selector stage does not define the output lane '{}' associated with predicate '{}'"),
  LIB_0013("Failed to validate the Selector predicate '{}', {}"),
  LIB_0014("Failed to evaluate the Selector predicate '{}', {}"),
  LIB_0015("Record did not match any predicate"),
  LIB_0016("Record does not satisfy any predicate, failing pipeline. Record '{}'"),

  // AbstractSpoolDirSource
  LIB_0100("Could not archive file '{}' in error, {}"),
  LIB_0101("Error while processing file '{}' at position '{}', {}"),

  // JsonSpoolDirSource
  LIB_0200("Discarding Json Object '{}', it exceeds maximum length '{}', file '{}', object starts at offset '{}'"),

  //Kafka source
  LIB_0300("A message with offset '{}' which is greater than the latest offset '{}' is requested from Kafka topic '{}' partition '{}'."),


  //Field Type Converter Processor
  LIB_0400("Failed to convert value '{}' to type '{}', {}."),
  ;

  private final String msg;

  StageLibError(String msg) {
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
