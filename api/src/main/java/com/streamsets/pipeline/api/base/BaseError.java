/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.ErrorCode;

public enum BaseError implements ErrorCode {
  BASE_0001("Stage '{}', there should be 1 output lane but there are '{}'"),
  BASE_0002("Cannot convert {} '{}' to a boolean"),
  BASE_0003("Cannot convert {} '{}' to a byte[]"),
  BASE_0004("Cannot convert byte[] to other type, {}"),
  BASE_0005("Cannot convert {} '{}' to a byte"),
  BASE_0006("Cannot convert {} '{}' to a char"),
  BASE_0007("Cannot parse '{}' to a Date, format must be ISO8601 UTC (yyyy-MM-dd'T'HH:mm'Z')"),
  BASE_0008("Cannot convert {} '{}' to a Date"),
  BASE_0009("Cannot convert {} '{}' to a BigDecimal"),
  BASE_0010("Cannot convert {} '{}' to a double"),
  BASE_0011("Cannot convert {} '{}' to a float"),
  BASE_0012("Cannot convert {} '{}' to a int"),
  BASE_0013("Cannot convert {} '{}' to a List"),
  BASE_0014("Cannot convert List to other type, {}"),
  Base_0015("Cannot convert {} '{}' to a long"),
  Base_0016("Cannot convert {} '{}' to a Map"),
  Base_0017("Cannot convert Map to other type, {}"),
  BASE_0018("Cannot convert {} '{}' to a short"),


;

  private final String msg;

  BaseError(String msg) {
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
