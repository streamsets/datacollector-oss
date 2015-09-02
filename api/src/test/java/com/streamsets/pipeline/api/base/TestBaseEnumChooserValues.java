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
