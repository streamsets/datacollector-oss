/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Locale;

public class TestLocaleInContext {

  @Before
  public void cleanUp() {
    LocaleInContext.set(null);
  }

  @Test
  public void testFirstUseInThread() throws Exception {
    Thread t = new Thread() {
      @Override
      public void run() {
        Assert.assertEquals(Locale.getDefault(), LocaleInContext.get());
      }
    };
    t.start();
    t.join();
  }

  @Test
  public void testSet() {
    Locale locale = Locale.forLanguageTag("xyz");
    LocaleInContext.set(locale);
    Assert.assertEquals(locale, LocaleInContext.get());
    LocaleInContext.set(null);
    Assert.assertEquals(Locale.getDefault(), LocaleInContext.get());
  }

}
