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
import java.util.List;

public class TestBaseEnumChooserValues {

  public enum EnumTest { A, B}

  public enum EnumWithLabelTest implements Label {
    X;

    @Override
    public String getLabel() {
      return "x";
    }

  }

  public class EnumTestChooserValues extends BaseEnumChooserValues {
    public EnumTestChooserValues() {
      super(EnumTest.class);
    }
  }

  public class EnumWithLabelTestChooserValues extends BaseEnumChooserValues {
    public EnumWithLabelTestChooserValues() {
      super(EnumWithLabelTest.class);
    }
  }

  @Test
  public void testEnum() {
    ChooserValues cv = new EnumTestChooserValues();
    Assert.assertEquals(Arrays.asList("A", "B"), cv.getValues());
    Assert.assertEquals(Arrays.asList("A", "B"), cv.getLabels());

    // asserting the resolve is done on first use only
    List<String> values = cv.getValues();
    List<String> labels = cv.getLabels();
    ChooserValues cv1 = new EnumTestChooserValues();
    Assert.assertSame(values, cv1.getValues());
    Assert.assertSame(labels, cv1.getLabels());
  }

  @Test
  public void testEnumWithLabel() {
    ChooserValues cv = new EnumWithLabelTestChooserValues();
    Assert.assertEquals(Arrays.asList("X"), cv.getValues());
    Assert.assertEquals(Arrays.asList("x"), cv.getLabels());
  }

}
