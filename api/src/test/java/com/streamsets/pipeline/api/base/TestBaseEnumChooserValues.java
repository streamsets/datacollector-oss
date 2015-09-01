/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.Label;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class TestBaseEnumChooserValues {

  public enum EnumTest { A, B}

  public enum EnumWithLabelTest implements Label {
    X, Y;

    @Override
    public String getLabel() {
      return name().toLowerCase();
    }

  }

  public class EnumTestChooserValues extends BaseEnumChooserValues<EnumTest> {
    public EnumTestChooserValues() {
      super(EnumTest.class);
    }
  }

  public class EnumWithLabelTestChooserValues extends BaseEnumChooserValues<EnumWithLabelTest> {
    public EnumWithLabelTestChooserValues() {
      super(EnumWithLabelTest.class);
    }
  }

  @Test
  public void testEnum() {
    ChooserValues cv = new EnumTestChooserValues();
    Assert.assertEquals(Arrays.asList("A", "B"), cv.getValues());
    Assert.assertEquals(Arrays.asList("A", "B"), cv.getLabels());
  }

  @Test
  public void testEnumWithLabel() {
    ChooserValues cv = new EnumWithLabelTestChooserValues();
    Assert.assertEquals(Arrays.asList("X", "Y"), cv.getValues());
    Assert.assertEquals(Arrays.asList("x", "y"), cv.getLabels());
  }

  @Test
  public void testEnumSubSet() {
    ChooserValues cv = new BaseEnumChooserValues<EnumTest>(EnumTest.A){};
    Assert.assertEquals(Collections.singletonList("A"), cv.getValues());
    Assert.assertEquals(Collections.singletonList("A"), cv.getLabels());
  }

  @Test
  public void testEnumWithLabelSubSet() {
    ChooserValues cv = new BaseEnumChooserValues<EnumWithLabelTest>(EnumWithLabelTest.X){};
    Assert.assertEquals(Collections.singletonList("X"), cv.getValues());
    Assert.assertEquals(Collections.singletonList("x"), cv.getLabels());
  }

}
