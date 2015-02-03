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
  LIB_0007("Path '{}' is no a file"),

  LIB_0003("LogDatProducer file='{}' offset='{}', {}"),

  LIB_0005("Error parsing a JSON string '{}': {}"),

  LIB_0006("Invalid configuration, name='{}', value='{}'"),

  // Selector
  LIB_0010("Selector must have at least 1 output stream"),
  LIB_0011("There are more output streams '{}' than stream-conditions '{}'"),
  LIB_0012("The Selector stage does not define the output stream '{}' associated with condition '{}'"),
  LIB_0013("Failed to validate the Selector condition '{}', {}"),
  LIB_0014("Failed to evaluate the Selector condition '{}', {}"),
  LIB_0015("Record did not match any condition"),
  LIB_0016("Record does not satisfy any condition, failing pipeline. Record '{}'"),

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

  //JSONParser processor
  LIB_0800("Record '{}' does not have the field-path '{}', cannot parse"),
  LIB_0801("Record '{}' has the field-path '{}' set to NULL, cannot parse"),
  LIB_0802("Record '{}' cannot set the parsed JSON  at field-path='{}', field-path does not exist"),

  //De-dup processor
  LIB_0900("Maximum record count must be greater than zero, it is '{}'"),
  LIB_0901("Time window must be zero (disabled) or greater than zero, it is '{}'"),
  LIB_0902("At least one field-path to hash must be specified"),
  LIB_0903("The estimated required memory for '{}' records is '{}'MBs, current maximum heap is '{}'MBs, the " +
           "required memory must not exceed 1/3 of the maximum heap"),

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
