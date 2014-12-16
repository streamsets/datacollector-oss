/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Locale;

public class TestLocalizableMessage {

  @Test
  public void testNoBundle() {
    LocalizableMessage lm = new LocalizableMessage("no-bundle", "key", "DEFAULT '{}'", new Object[]{"value"});
    Assert.assertEquals("DEFAULT 'value'", lm.getNonLocalized());
    Assert.assertEquals("DEFAULT 'value'", lm.getLocalized());
  }

  @Test
  public void testBundle() {
    LocalizableMessage lm = new LocalizableMessage("TestLocalizableMessage", "key", "DEFAULT '{}'",
                                                   new Object[]{"value"});
    Assert.assertEquals("DEFAULT 'value'", lm.getNonLocalized());
    Assert.assertEquals("BUNDLE 'value'", lm.getLocalized());
  }

  @Test
  public void testBundleWithLocale() {
    LocaleInContext.set(Locale.forLanguageTag("xyz"));
    try {
      LocalizableMessage lm = new LocalizableMessage("TestLocalizableMessage", "key", "DEFAULT '{}'",
                                                     new Object[]{"value"});
      Assert.assertEquals("DEFAULT 'value'", lm.getNonLocalized());
      Assert.assertEquals("BUNDLE 'value' xyz", lm.getLocalized());
    } finally {
      LocaleInContext.set(Locale.getDefault());
    }
  }

  @Test
  public void testBundleWithClassLoaderAndLocale() {
    LocaleInContext.set(Locale.forLanguageTag("xyz"));
    try {
      LocalizableMessage lm = new LocalizableMessage(Thread.currentThread().getContextClassLoader(),
                                                     "TestLocalizableMessage", "key", "DEFAULT '{}'",
                                                     new Object[]{"value"});
      Assert.assertEquals("DEFAULT 'value'", lm.getNonLocalized());
      Assert.assertEquals("BUNDLE 'value' xyz", lm.getLocalized());
    } finally {
      LocaleInContext.set(Locale.getDefault());
    }
  }

  @Test
  public void testNoBundleWithClassLoader() {
    LocaleInContext.set(Locale.forLanguageTag("xyz"));
    try {
      LocalizableMessage lm = new LocalizableMessage(new URLClassLoader(new URL[0], null),
                                                     "TestLocalizableMessage", "key", "DEFAULT '{}'",
                                                     new Object[]{"value"});
      Assert.assertEquals("DEFAULT 'value'", lm.getNonLocalized());
      Assert.assertEquals("DEFAULT 'value'", lm.getLocalized());
    } finally {
      LocaleInContext.set(Locale.getDefault());
    }
  }
}
