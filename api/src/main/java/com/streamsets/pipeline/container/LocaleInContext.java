/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import java.util.Locale;

public abstract class LocaleInContext {
  private static final ThreadLocal<Locale> LOCALE_TL = new ThreadLocal<Locale>() {
    @Override
    protected Locale initialValue() {
      return Locale.getDefault();
    }
  };

  public static void set(Locale locale) {
    LOCALE_TL.set((locale != null) ? locale : Locale.getDefault());
  }

  public static Locale get() {
    return LOCALE_TL.get();
  }

}
