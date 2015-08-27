/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.lib;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum DataFormatErrors implements ErrorCode {
  // Configuration errors
  DATA_FORMAT_01("Max data object length cannot be less than 1"),
  DATA_FORMAT_02("XML delimiter element cannot be empty"),
  DATA_FORMAT_03("Invalid XML element name '{}'"),
  DATA_FORMAT_04("Unsupported data format '{}'"),
  DATA_FORMAT_05("Unsupported charset '{}'"),
  DATA_FORMAT_06("Cannot create the parser factory: {}"),

  ;
  private final String msg;

  DataFormatErrors(String msg) {
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