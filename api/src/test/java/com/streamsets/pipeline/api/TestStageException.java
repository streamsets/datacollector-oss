/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

import com.streamsets.pipeline.api.base.BaseError;
import com.streamsets.pipeline.container.LocaleInContext;
import com.streamsets.pipeline.container.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;

public class TestStageException {

  @After
  public void cleanUp() {
    LocaleInContext.set(null);
  }

  @Test
  public void testException() {
    StageException ex = new StageException(BaseError.BASE_0001);
    Assert.assertEquals(BaseError.BASE_0001, ex.getErrorCode());
    Assert.assertEquals("BASE_0001 - " + BaseError.BASE_0001.getMessage(), ex.getMessage());
    LocaleInContext.set(Locale.forLanguageTag("abc"));
    Assert.assertEquals("BASE_0001 - " + BaseError.BASE_0001.getMessage(), ex.getMessage());
    LocaleInContext.set(Locale.forLanguageTag("xyz"));
    Assert.assertEquals("BASE_0001 - Hello XYZ '{}'", ex.getLocalizedMessage());
    LocaleInContext.set(null);
    Assert.assertNull(ex.getCause());

    Exception cause = new Exception();
    ex = new StageException(BaseError.BASE_0001, cause);
    Assert.assertEquals(cause, ex.getCause());

    ex = new StageException(BaseError.BASE_0001, "a");
    Assert.assertEquals("BASE_0001 - " + Utils.format(BaseError.BASE_0001.getMessage(), "a"), ex.getMessage());
    LocaleInContext.set(Locale.forLanguageTag("xyz"));
    Assert.assertEquals("BASE_0001 - Hello XYZ 'a'", ex.getLocalizedMessage());
    LocaleInContext.set(null);
    Assert.assertNull(ex.getCause());

    ex = new StageException(BaseError.BASE_0001, "a", 1, cause);
    Assert.assertEquals("BASE_0001 - " + Utils.format(BaseError.BASE_0001.getMessage(), "a", 1), ex.getMessage());
    LocaleInContext.set(Locale.forLanguageTag("xyz"));
    Assert.assertEquals("BASE_0001 - Hello XYZ 'a'", ex.getLocalizedMessage());
    LocaleInContext.set(null);
    Assert.assertEquals(cause, ex.getCause());

  }

}
