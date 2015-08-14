/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.flume;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum FlumeErrors implements ErrorCode {
  //Flume Origin

  //Flume target messages
  FLUME_50("Error serializing record '{}': {}"),
  FLUME_51("Unable to send data to flume, reason : {}"),
  FLUME_52("Unable to send data to flume because the pipeline was stopped."),

  //Flume ValidationValidation
  FLUME_101("Flume Host Configuration cannot be empty"),
  FLUME_102("Flume Host Alias cannot be empty"),
  FLUME_103("Invalid Flume Host Configuration '{}'"),
  FLUME_104("Configuration '{}' cannot be less than '{}'"),

  ;
  private final String msg;

  FlumeErrors(String msg) {
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
