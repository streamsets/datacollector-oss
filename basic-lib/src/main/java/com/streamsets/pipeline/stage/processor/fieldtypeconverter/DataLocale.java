/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.Label;

import java.util.Locale;

public enum DataLocale implements Label {

  ENGLISH(Locale.ENGLISH),
  FRENCH(Locale.FRENCH),
  GERMAN(Locale.GERMAN),
  ITALIAN(Locale.ITALIAN),
  JAPANESE(Locale.JAPANESE),
  KOREAN(Locale.KOREAN),
  CHINESE(Locale.CHINESE),
  SIMPLIFIED_CHINESE(Locale.SIMPLIFIED_CHINESE),
  TRADITIONAL_CHINESE(Locale.TRADITIONAL_CHINESE);

  private final Locale locale;

  private DataLocale(Locale locale) {
    this.locale = locale;
  }

  public Locale getLocale() {
    return this.locale;
  }


  @Override
  public String getLabel() {
    return locale.getDisplayName();
  }
}
