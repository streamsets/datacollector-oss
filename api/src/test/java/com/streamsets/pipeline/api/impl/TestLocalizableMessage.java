/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
