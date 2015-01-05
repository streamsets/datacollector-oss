/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.devtest.fieldtypeconverter;

public enum DateMask {

  YYYY_MM_DD("yyyy-MM-dd"),
  DD_MM_YYYY("dd-MM-YYYY"),
  YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"),
  YYYY_MM_DD_HH_MM_SS_SSS("yyyy-MM-dd HH:mm:ss.SSS"),
  YYYY_MM_DD_HH_MM_SS_SSS_Z("yyyy-MM-dd HH:mm:ss.SSS Z");

  private String format;

  private DateMask(String format) {
    this.format = format;
  }

  public String getFormat() {
    return format;
  }
}
