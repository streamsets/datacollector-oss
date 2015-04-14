/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FlumeTestUtil {

  private static final String MIME = "text/plain";
  private static final String TEST_STRING = "Hello World";

  public static List<Record> produce20Records() throws IOException {
    List<Record> list = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Record record = RecordCreator.create();
      Map<String, Field> map = new HashMap<>();
      map.put("name", Field.create("NAME" + i));
      map.put("lastStatusChange", Field.create(i));
      record.set(Field.create(map));
      list.add(record);
    }
    return list;
  }

  public static List<Record> createJsonRecords() throws IOException {
    return produce20Records();
  }

  public static List<Record> createStringRecords() {
    List<Record> records = new ArrayList<>(9);
    for (int i = 0; i < 9; i++) {
      Record r = RecordCreator.create("s", "s:1", (TEST_STRING + i).getBytes(), MIME);
      r.set(Field.create((TEST_STRING + i)));
      records.add(r);
    }
    return records;
  }

  public static List<Record> createCsvRecords() throws IOException {
    List<Record> records = new ArrayList<>();
    String line;
    BufferedReader bufferedReader = new BufferedReader(new FileReader(FlumeTestUtil.class.getClassLoader()
      .getResource("testFlumeTarget.csv").getFile()));
    while ((line = bufferedReader.readLine()) != null) {
      String columns[] = line.split(",");
      List<Field> list = new ArrayList<>();
      for (String column : columns) {
        Map<String, Field> map = new LinkedHashMap<>();
        map.put("value", Field.create(column));
        list.add(Field.create(map));
      }
      Record record = RecordCreator.create("s", "s:1", null, null);
      record.set(Field.create(list));
      records.add(record);
    }
    return records;
  }
}
