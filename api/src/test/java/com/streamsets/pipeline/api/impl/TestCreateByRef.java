/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;

public class TestCreateByRef {


  @Test
  public void testByValue() {
    List<Field> list = ImmutableList.of(Field.create(1));
    Field f = Field.create(list);
    Assert.assertEquals(list, f.getValueAsList());
    Assert.assertNotSame(list, f.getValueAsList());
  }

  @Test
  public void testByRef() throws Exception {
    CreateByRef.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        List<Field> list = ImmutableList.of(Field.create(1));
        Field f = Field.create(list);
        Assert.assertEquals(list, f.getValueAsList());
        Assert.assertSame(list, f.getValueAsList());
        return null;
      }
    });
  }

}
