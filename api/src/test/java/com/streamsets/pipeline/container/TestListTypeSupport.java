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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public void testSnapshot1() {
    ListTypeSupport support = new ListTypeSupport();
    List<Field> m = new ArrayList<>();
    Assert.assertEquals(m, support.snapshot(m));
    Assert.assertNotSame(m, support.snapshot(m));
  }

  @Test
  public void testSnapshot2() {
    ListTypeSupport support = new ListTypeSupport();
    List<Field> m = new ArrayList<>();
    Field f1 = Field.create(true);
    Field f2 = Field.createDate(new Date());
    m.add(f1);
    m.add(f2);
    Assert.assertEquals(m, support.snapshot(m));
    Assert.assertNotSame(m, support.snapshot(m));
    Assert.assertEquals(f1, ((List) support.snapshot(m)).get(0));
    Assert.assertEquals(f2, ((List)support.snapshot(m)).get(1));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSnapshot3() {
    ListTypeSupport support = new ListTypeSupport();
    List<Field> m = new ArrayList<>();
    Field f1 = Field.create(true);
    Field f2 = Field.createDate(new Date());
    m.add(f1);
    m.add(f2);
    List<Field> m2 = new ArrayList<>();
    m2.add(Field.create(m));
    Assert.assertEquals(m2, support.snapshot(m2));
    Assert.assertNotSame(m2, support.snapshot(m2));
    Assert.assertEquals(f1, ((List<Field>) ((List<Field>) support.snapshot(m2)).get(0).getValue()).get(0));
    Assert.assertEquals(f2,
                        ((List<Field>) ((List<Field>) support.snapshot(m2)).get(0).getValue()).get(1));

    Assert.assertEquals(m, ((List<Field>) support.snapshot(m2)).get(0).getValue());
    Assert.assertNotSame(m, ((List<Field>) support.snapshot(m2)).get(0).getValue());
  }

}
