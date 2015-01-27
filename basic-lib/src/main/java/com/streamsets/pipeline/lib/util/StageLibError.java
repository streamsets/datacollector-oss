/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.ErrorCode;

public enum StageLibError implements ErrorCode {

  // LogTailSource
  LIB_0001("Log File '{}' does not exist"),
  LIB_0002("Insufficient permissions to read the log file '{}'"),

  LIB_0003("LogDatProducer file='{}' offset='{}', {}"),

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
  LIB_0200("Discarding Json Object, it exceeds maximum length '{}', file '{}', object starts at offset '{}'"),

  // XmlSpoolDirSource
  LIB_0300("Discarding Xml Object, it exceeds maximum length '{}', file '{}', object starts at offset '{}'"),

  //Field Type Converter Processor
  LIB_0400("Failed to convert value '{}' to type '{}', {}."),

  //Field Hasher Processor
  LIB_0500("Error creating message digest for {}, {}."),

  //Expression processor
  LIB_0600("Failed to evaluate expression '{}', {}"),

  //Splitter processor
  LIB_0700("The number of splits fields must be greater than one"),
  LIB_0701("The record '{}' does not have the field-path '{}', cannot split"),
  LIB_0702("The record '{}' does not have enough splits"),

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
