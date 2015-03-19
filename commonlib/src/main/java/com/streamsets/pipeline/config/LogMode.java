/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum LogMode implements Label {

  COMMON_LOG_FORMAT("Common Log Format"),
  COMBINED_LOG_FORMAT("Combined Log Format"),
  APACHE_ERROR_LOG_FORMAT("Apache Error Log Format"),
  APACHE_CUSTOM_LOG_FORMAT("Apache Access Log Custom Format"),
  REGEX("Regular Expression"),

  ;

  private final String label;

  LogMode(String label) {
    this.label = label;

  }

  @Override
  public String getLabel() {
    return label;
  }

}
