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
