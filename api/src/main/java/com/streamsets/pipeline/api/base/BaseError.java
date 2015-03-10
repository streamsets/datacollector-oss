/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.ErrorCode;

public enum BaseError implements ErrorCode {
  BASE_0001("Stage '{}' requires at least one output stream. There are '{}'."),
  BASE_0002("Cannot convert {} field '{}' to Boolean"),
  BASE_0003("Cannot convert {} field '{}' to Byte[]"),
  BASE_0004("Cannot convert Byte[] to {}"),
  BASE_0005("Cannot convert {} field '{}' to Byte"),
  BASE_0006("Cannot convert {} field '{}' to Char"),
  BASE_0007("Cannot parse '{}' to a Date. Use the following ISO 8601 UTC date format: yyyy-MM-dd'T'HH:mm'Z'."),
  BASE_0008("Cannot convert {} field '{}' to Date"),
  BASE_0009("Cannot convert {} field '{}' to Decimal"),
  BASE_0010("Cannot convert {} field '{}' to Double"),
  BASE_0011("Cannot convert {} field '{}' to Float"),
  BASE_0012("Cannot convert {} field '{}' to Integer"),
  BASE_0013("Cannot convert {} field '{}' to List"),
  BASE_0014("Cannot convert List to {}"),
  Base_0015("Cannot convert {} field '{}' to Long"),
  Base_0016("Cannot convert {} field '{}' to Map"),
  Base_0017("Cannot convert Map to {}"),
  BASE_0018("Cannot convert {} field '{}' to Short"),
  Base_0019("Cannot convert Map, List, or Byte[] to String"),


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
