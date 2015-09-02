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

import java.math.BigDecimal;

public class TestBooleanTypeSupport {

  @Test
  public void testCreate() {
    BooleanTypeSupport ts = new BooleanTypeSupport();
    Boolean o = true;
    Assert.assertSame(o, ts.create(o));
  }

  @Test
  public void testGet() {
    BooleanTypeSupport ts = new BooleanTypeSupport();
    Boolean o = true;
    Assert.assertSame(o, ts.get(o));
  }

  @Test
  public void testClone() {
    BooleanTypeSupport ts = new BooleanTypeSupport();
    Boolean o = true;
    Assert.assertSame(o, ts.clone(o));
  }

  @Test
  public void testConvertValid() {
    BooleanTypeSupport support = new BooleanTypeSupport();
    Assert.assertEquals(true, support.convert(true));
    Assert.assertEquals(true, support.convert("true"));
    Assert.assertEquals(true, support.convert(1));
    Assert.assertEquals(true, support.convert((long) 1));
    Assert.assertEquals(true, support.convert((short) 1));
    Assert.assertEquals(true, support.convert((byte) 1));
    Assert.assertEquals(true, support.convert((float) 1));
    Assert.assertEquals(true, support.convert((double) 1));
    Assert.assertEquals(true, support.convert(new BigDecimal(1)));

    Assert.assertEquals(false, support.convert(false));
    Assert.assertEquals(false, support.convert("false"));
    Assert.assertEquals(false, support.convert(0));
    Assert.assertEquals(false, support.convert((long) 0));
    Assert.assertEquals(false, support.convert((short) 0));
    Assert.assertEquals(false, support.convert((byte) 0));
    Assert.assertEquals(false, support.convert((float) 0));
    Assert.assertEquals(false, support.convert((double) 0));
    Assert.assertEquals(false, support.convert(new BigDecimal(0)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid() {
    new BooleanTypeSupport().convert(new Exception());
  }

}
