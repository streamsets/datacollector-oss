/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum DateFormat implements Label {

  YYYY_MM_DD("yyyy-MM-dd", "yyyy-MM-dd"),
  DD_MM_YYYY("dd-MMM-yyyy", "dd-MMM-yyyy"),
  YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss"),
  YYYY_MM_DD_HH_MM_SS_SSS("yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss.SSS"),
  YYYY_MM_DD_HH_MM_SS_SSS_Z("yyyy-MM-dd HH:mm:ss.SSS Z", "yyyy-MM-dd HH:mm:ss.SSS Z"),
  YYYY_MM_DD_T_HH_MM_Z("yyyy-MM-dd'T'HH:mm'Z'", "yyyy-MM-dd'T'HH:mm'Z'"),
  YYYY_MM_DD_T_HH_MM_SS_SSS_Z("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
  OTHER(null, "Other ...");

  private final String format;
  private final String label;

  private DateFormat(String format, String label) {
    this.format = format;
    this.label = label;
  }

  public String getFormat() {
    return format;
  }


  @Override
  public String getLabel() {
    return label;
  }
}
