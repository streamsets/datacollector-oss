/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
