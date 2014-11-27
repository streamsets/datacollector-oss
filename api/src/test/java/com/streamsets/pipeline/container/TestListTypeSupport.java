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
package com.streamsets.pipeline.container;

import com.streamsets.pipeline.api.Field;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TestListTypeSupport {

  @Test
  public void testConvertValid() throws Exception {
    ListTypeSupport support = new ListTypeSupport();
    List<Field> m = new ArrayList<>();
    Assert.assertEquals(m, support.convert(m));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid() {
    new ListTypeSupport().convert(new Exception());
  }

  @Test
  public void testConstructorCopy() {
    ListTypeSupport support = new ListTypeSupport();
    List<Field> l = new ArrayList<>();
    Field f1 = Field.create(true);
    Field f2 = Field.createDate(new Date());
    l.add(f1);
    l.add(f2);
    List<Field> l2 = new ArrayList<>();
    l2.add(Field.create(true));
    Field f3 = Field.create(l2);
    l.add(f3);
    Object copy = support.constructorCopy(l);
    Assert.assertEquals(l, copy);
    Assert.assertNotSame(l, copy);
    Assert.assertEquals(f1, ((List) copy).get(0));
    Assert.assertEquals(f2, ((List) copy).get(1));
    Assert.assertEquals(f3, ((List) copy).get(2));
    Assert.assertNotSame(f3, ((List) copy).get(2));
  }

  @Test
  public void testGetReference() {
    ListTypeSupport support = new ListTypeSupport();
    List<Field> m = new ArrayList<>();
    Field f1 = Field.create(true);
    Field f2 = Field.createDate(new Date());
    m.add(f1);
    m.add(f2);
    Assert.assertEquals(m, support.getReference(m));
    Assert.assertSame(m, support.getReference(m));
    Assert.assertEquals(f1, ((List) support.getReference(m)).get(0));
    Assert.assertEquals(f2, ((List)support.getReference(m)).get(1));
  }

}
