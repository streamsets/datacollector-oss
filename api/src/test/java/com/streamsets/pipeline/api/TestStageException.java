/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

import com.streamsets.pipeline.api.base.Errors;
import com.streamsets.pipeline.api.impl.LocaleInContext;
import com.streamsets.pipeline.api.impl.Utils;
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
    StageException ex = new StageException(Errors.API_00);
    Assert.assertEquals(Errors.API_00, ex.getErrorCode());
    Assert.assertEquals("API_00 - " + Errors.API_00.getMessage(), ex.getMessage());
    LocaleInContext.set(Locale.forLanguageTag("abc"));
    Assert.assertEquals("API_00 - " + Errors.API_00.getMessage(), ex.getMessage());
    LocaleInContext.set(Locale.forLanguageTag("xyz"));
    Assert.assertEquals("API_00 - Hello XYZ '{}'", ex.getLocalizedMessage());
    LocaleInContext.set(null);
    Assert.assertNull(ex.getCause());

    Exception cause = new Exception();
    ex = new StageException(Errors.API_00, cause);
    Assert.assertEquals(cause, ex.getCause());

    ex = new StageException(Errors.API_00, "a");
    Assert.assertEquals("API_00 - " + Utils.format(Errors.API_00.getMessage(), "a"), ex.getMessage());
    LocaleInContext.set(Locale.forLanguageTag("xyz"));
    Assert.assertEquals("API_00 - Hello XYZ 'a'", ex.getLocalizedMessage());
    LocaleInContext.set(null);
    Assert.assertNull(ex.getCause());

    ex = new StageException(Errors.API_00, "a", 1, cause);
    Assert.assertEquals("API_00 - " + Utils.format(Errors.API_00.getMessage(), "a", 1), ex.getMessage());
    LocaleInContext.set(Locale.forLanguageTag("xyz"));
    Assert.assertEquals("API_00 - Hello XYZ 'a'", ex.getLocalizedMessage());
    LocaleInContext.set(null);
    Assert.assertEquals(cause, ex.getCause());

  }

}
