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

import com.streamsets.pipeline.api.Field;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestFieldDeepCopy {

  @Test
  @SuppressWarnings("unchecked")
  public void testClone() {
    ListTypeSupport ts = new ListTypeSupport();
    List<Field> list = new ArrayList<>();
    list.add(Field.create("a"));
    List<Field> cList = (List) ts.clone(list);
    Assert.assertEquals(list, cList);
    Assert.assertNotSame(list, cList);
    Assert.assertSame(list.get(0), cList.get(0));
    List<Field> listX = new ArrayList<>();
    listX.add(Field.create(list));

    Map<String, Field> map = new HashMap<>();
    map.put("list", Field.create(listX));
    MapTypeSupport ms = new MapTypeSupport();
    Map<String, Field> cMap = (Map) ms.clone(map);
    Assert.assertEquals(map, cMap);
    Assert.assertNotSame(map, cMap);
    Assert.assertEquals(map.get("list"), cMap.get("list"));
    Assert.assertNotSame(map.get("list"), cMap.get("list"));
    Assert.assertEquals(map.get("list").getValueAsList().get(0), cMap.get("list").getValueAsList().get(0));
    Assert.assertNotSame(map.get("list").getValueAsList().get(0), cMap.get("list").getValueAsList().get(0));

    LinkedHashMap<String, Field> listMap = new LinkedHashMap<>();
    listMap.put("list", Field.create(listX));
    ListMapTypeSupport listMapTypeSupport = new ListMapTypeSupport();
    LinkedHashMap<String, Field> listMapClone = (LinkedHashMap) listMapTypeSupport.clone(listMap);
    Assert.assertEquals(listMap, listMapClone);
    Assert.assertNotSame(listMap, listMapClone);
    Assert.assertEquals(map.get("list"), listMapClone.get("list"));
    Assert.assertNotSame(map.get("list"), listMapClone.get("list"));
    Assert.assertEquals(map.get("list").getValueAsList().get(0), listMapClone.get("list").getValueAsList().get(0));
    Assert.assertNotSame(map.get("list").getValueAsList().get(0), listMapClone.get("list").getValueAsList().get(0));
  }

}
