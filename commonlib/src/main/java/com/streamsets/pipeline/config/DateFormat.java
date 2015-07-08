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

  YYYY_MM_DD("yyyy-MM-dd", "YYYY-MM-DD"),
  DD_MM_YYYY("dd-MMM-YYYY", "DD-MMM-YYYY"),
  YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss", "YYYY-MM-DD hh:mm:ss"),
  YYYY_MM_DD_HH_MM_SS_SSS("yyyy-MM-dd HH:mm:ss.SSS", "YYYY-MM-DD hh:mm:ss.sss"),
  YYYY_MM_DD_HH_MM_SS_SSS_Z("yyyy-MM-dd HH:mm:ss.SSS Z", "YYYY-MM-DD hh:mm:ss.sss Z"),
  YYYY_MM_DD_T_HH_MM_Z("yyyy-MM-dd'T'HH:mm'Z'", "YYYY-MM-DD'T'hh:mm'Z'"),
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
