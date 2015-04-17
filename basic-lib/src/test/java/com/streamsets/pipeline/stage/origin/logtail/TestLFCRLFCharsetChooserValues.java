/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.config.CharsetChooserValues;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class TestLFCRLFCharsetChooserValues {

  @Test
  public void testChooserValues() {
    CharsetChooserValues ccv = new LFCRLFCharsetChooserValues();
    Set<String> charsets = new HashSet<>(ccv.getValues());
    Assert.assertTrue(charsets.contains("UTF-8"));
    Assert.assertFalse(charsets.contains("UTF-16"));
  }
}
