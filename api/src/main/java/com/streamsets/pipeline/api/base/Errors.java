/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
public enum Errors implements ErrorCode {
  API_00("Stage '{}' requires at least one output stream. There are '{}'."),
  API_01("Cannot convert {} field '{}' to Boolean"),
  API_02("Cannot convert {} field '{}' to Byte[]"),
  API_03("Cannot convert Byte[] to {}"),
  API_04("Cannot convert {} field '{}' to Byte"),
  API_05("Cannot convert {} field '{}' to Char"),
  API_06("Cannot parse '{}' to a Date. Use the following ISO 8601 UTC date format: yyyy-MM-dd'T'HH:mm'Z'."),
  API_07("Cannot convert {} field '{}' to Date"),
  API_08("Cannot convert {} field '{}' to Decimal"),
  API_09("Cannot convert {} field '{}' to Double"),
  API_10("Cannot convert {} field '{}' to Float"),
  API_11("Cannot convert {} field '{}' to Integer"),
  API_12("Cannot convert {} field '{}' to List"),
  API_13("Cannot convert List to {}"),
  API_14("Cannot convert {} field '{}' to Long"),
  API_15("Cannot convert {} field '{}' to Map"),
  API_16("Cannot convert Map to {}"),
  API_17("Cannot convert {} field '{}' to Short"),
  API_18("Cannot convert Map, List, or Byte[] to String"),

  API_19("Error while initializing stage: {}"),
  API_20("The stage implementation overridden the init() but didn't call super.init()")

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
