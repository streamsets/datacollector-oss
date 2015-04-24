/*
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

/**
 * Enum for representing possible HTTP methods
 */
@GenerateResourceBundle
public enum HttpMethod implements Label {
  GET("GET"),
  PUT("PUT"),
  POST("POST"),
  DELETE("DELETE"),
  HEAD("HEAD")
  ;

  private final String label;

  HttpMethod(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}