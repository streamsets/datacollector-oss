/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.execution.alerts;

import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestUtil {


  private static final String TEST_STRING = "TestAlertsChecker";
  private static final String MIME = "application/octet-stream";

  public static Map<String, Map<String, List<Record>>> createSnapshot(String lane, String ruleId) {
    Map<String, List<Record>> snapshot = new HashMap<>();
    List<Record> records = new ArrayList<>();

    Map<String, Field> map1 = new LinkedHashMap<>();
    map1.put("name", Field.create(Field.Type.STRING, "streamsets"));
    map1.put("zip", Field.create(Field.Type.INTEGER, 94102));
    Record r1 = new RecordImpl("s", "s:2", TEST_STRING.getBytes(), MIME);
    r1.set(Field.create(map1));
    records.add(r1);

    Map<String, Field> map2 = new LinkedHashMap<>();
    map2.put("name", Field.create(Field.Type.STRING, null));
    map2.put("zip", Field.create(Field.Type.INTEGER, 94102));
    Record r2 = new RecordImpl("s", "s:2", TEST_STRING.getBytes(), MIME);
    r2.set(Field.create(map2));
    records.add(r2);

    Map<String, Field> map3 = new LinkedHashMap<>();
    map3.put("name", Field.create(Field.Type.STRING, null));
    map3.put("zip", Field.create(Field.Type.INTEGER, 94102));
    Record r3 = new RecordImpl("s", "s:2", TEST_STRING.getBytes(), MIME);
    r3.set(Field.create(map3));
    records.add(r3);

    Map<String, Field> map4 = new LinkedHashMap<>();
    map4.put("name", Field.create(Field.Type.STRING, "streamsets"));
    map4.put("zip", Field.create(Field.Type.INTEGER, 94102));
    Record r4 = new RecordImpl("s", "s:2", TEST_STRING.getBytes(), MIME);
    r4.set(Field.create(map4));
    records.add(r4);

    Map<String, Field> map5 = new LinkedHashMap<>();
    map5.put("name", Field.create(Field.Type.STRING, null));
    map5.put("zip", Field.create(Field.Type.INTEGER, 94101));
    Record r5 = new RecordImpl("s", "s:2", TEST_STRING.getBytes(), MIME);
    r5.set(Field.create(map5));
    records.add(r5);

    Map<String, Field> map6 = new LinkedHashMap<>();
    map6.put("name", Field.create(Field.Type.STRING, "none"));
    map6.put("zip", Field.create(Field.Type.INTEGER, 94101));
    Record r6 = new RecordImpl("s", "s:2", TEST_STRING.getBytes(), MIME);
    r6.set(Field.create(map6));
    records.add(r6);

    snapshot.put(ruleId, records);
    Map<String, Map<String, List<Record>>> result = new HashMap<>();
    result.put(lane + "::s", snapshot);
    return result;
  }

  public static List<Record> createRecords(int n) {
    List<Record> records = new ArrayList<>();

    for(int i = 0; i < n; i++) {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(Field.Type.STRING, "streamsets"));
      map.put("zip", Field.create(Field.Type.INTEGER, 94102));
      Record r1 = new RecordImpl("s", "s:"+i, TEST_STRING.getBytes(), MIME);
      r1.set(Field.create(map));
      records.add(r1);
    }

    return records;
  }

  public static Map<String, Integer> createLaneToRecordSizeMap(String lane) {
    Map<String, Integer> map = new HashMap<>();
    map.put(lane + "::s", 6);
    return map;
  }

}
