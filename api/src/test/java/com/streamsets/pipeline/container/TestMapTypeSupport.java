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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TestMapTypeSupport {

  @Test
  public void testConvertValid() throws Exception {
    MapTypeSupport support = new MapTypeSupport();
    Map m = new HashMap<>();
    Assert.assertEquals(m, support.convert(m));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid() {
    new MapTypeSupport().convert(new Exception());
  }

  @Test
  public void testSnapshot1() {
    MapTypeSupport support = new MapTypeSupport();
    Map m = new HashMap<>();
    Assert.assertEquals(m, support.getReference(m));
    Assert.assertNotSame(m, support.getReference(m));
  }

  @Test
  public void testSnapshot2() {
    MapTypeSupport support = new MapTypeSupport();
    Map<String, Field> m = new HashMap<>();
    Field f1 = Field.create(true);
    Field f2 = Field.createDate(new Date());
    m.put("a", f1);
    m.put("b", f2);
    Assert.assertEquals(m, support.getReference(m));
    Assert.assertNotSame(m, support.getReference(m));
    Assert.assertEquals(f1, ((Map) support.getReference(m)).get("a"));
    Assert.assertEquals(f2, ((Map)support.getReference(m)).get("b"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSnapshot3() {
    MapTypeSupport support = new MapTypeSupport();
    Map<String, Field> m = new HashMap<>();
    Field f1 = Field.create(true);
    Field f2 = Field.createDate(new Date());
    m.put("a", f1);
    m.put("b", f2);
    Map<String, Field> m2 = new HashMap<>();
    m2.put("x", Field.create(m));
    Assert.assertEquals(m2, support.getReference(m2));
    Assert.assertNotSame(m2, support.getReference(m2));
    Assert.assertEquals(f1, ((Map<String, Field>) ((Map<String, Field>) support.getReference(m2)).get("x").getValue()).get("a"));
    Assert.assertEquals(f2,
                        ((Map<String, Field>) ((Map<String, Field>) support.getReference(m2)).get("x").getValue()).get("b"));

    Assert.assertEquals(m, ((Map<String, Field>) support.getReference(m2)).get("x").getValue());
    Assert.assertNotSame(m, ((Map<String, Field>) support.getReference(m2)).get("x").getValue());
  }

}
