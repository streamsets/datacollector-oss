/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  SPARK_01("Cannot parse record from message: {}"),
  SPARK_02("Batch Processing time cannot be less than 1"),
  SPARK_03("Invalid XML element name '{}'"),
  SPARK_04("Max data object length cannot be less than 1"),
  SPARK_05("Unsupported data format '{}'"),
  SPARK_06("Messages with XML data cannot have multiple XML documents in a single message"),
  SPARK_07("Unsupported charset '{}'"),
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
