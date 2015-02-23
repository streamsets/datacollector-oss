/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.streamsets.pipeline.api.ChooserMode;
import org.junit.Assert;
import org.junit.Test;

public class TestChooserModeBean {

  @Test
  public void testSuggested() {
    Assert.assertTrue(BeanHelper.wrapChooserMode(ChooserMode.SUGGESTED) ==
      ChooserModeJson.SUGGESTED);
    Assert.assertTrue(BeanHelper.unwrapChooserMode(ChooserModeJson.SUGGESTED) ==
      ChooserMode.SUGGESTED);
  }

  @Test
  public void testProvided() {
    Assert.assertTrue(BeanHelper.wrapChooserMode(ChooserMode.PROVIDED) ==
      ChooserModeJson.PROVIDED);
    Assert.assertTrue(BeanHelper.unwrapChooserMode(ChooserModeJson.PROVIDED) ==
      ChooserMode.PROVIDED);
  }
}
