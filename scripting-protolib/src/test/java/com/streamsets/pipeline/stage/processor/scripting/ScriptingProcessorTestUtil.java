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
package com.streamsets.pipeline.stage.processor.scripting;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.io.fileref.FileRefTestUtil;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Common verification of scripting processor unit tests.
 * Processor test classes should define their appropriate test script.
 * Subsequently this utility can be called to run and verify the results of the scripts.
 */
public class ScriptingProcessorTestUtil {

  public static String getScript(String scriptName, Class searchClass) {
    try {
      URL url = Resources.getResource(searchClass, scriptName);
      return Resources.toString(url, Charsets.UTF_8);
    } catch (IOException e) {
      System.out.println(e);
      return null;
    }
  }


  private ScriptingProcessorTestUtil() {}
  static final String JAVASCRIPT_CLASSNAME = "com.streamsets.pipeline.stage.processor.javascript.JavaScriptDProcessor";

  public static <C extends Processor> void verifyMapAndArray(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);
      assertEquals(4, output.getRecords().get("lane").size());
      Record outRec = output.getRecords().get("lane").get(0);
      assertEquals(Field.create((String) null), outRec.get());
      outRec = output.getRecords().get("lane").get(1);
      assertEquals(Field.Type.STRING, outRec.get("/").getType());
      assertEquals("Hello", outRec.get("/").getValue());
      outRec = output.getRecords().get("lane").get(2);
      assertEquals(Field.Type.MAP, outRec.get("/").getType());
      assertEquals(Field.Type.STRING, outRec.get("/foo").getType());
      assertEquals("FOO", outRec.get("/foo").getValue());
      outRec = output.getRecords().get("lane").get(3);
      assertEquals(Field.Type.LIST, outRec.get("/").getType());
      // JavaScript only defines "Number" as a type which is a 64-bit float (double)
      if (System.getProperty("java.version").startsWith("1.7.")) {
        assertEquals(Field.Type.DOUBLE, outRec.get("[0]").getType());
        assertEquals(5.0, outRec.get("[0]").getValue());
      }
      // Java8's Nashorn engine however, will respect the original Java type of Integer.
      if (System.getProperty("java.version").startsWith("1.8")) {
        assertEquals(Field.Type.INTEGER, outRec.get("[0]").getType());
        assertEquals(5, outRec.get("[0]").getValue());
      }
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyMode(Class<C> clazz, Processor processor) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {

      Record record1 = RecordCreator.create();
      record1.set(Field.create("Hello"));
      Record record2 = RecordCreator.create();
      record2.set(Field.create("Bye"));
      List<Record> input = Arrays.asList(record1, record2);
      StageRunner.Output output = runner.runProcess(input);
      assertEquals(2, output.getRecords().get("lane").size());
      assertEquals("Hello", output.getRecords().get("lane").get(0).get().getValueAsString());
      assertEquals("Bye", output.getRecords().get("lane").get(1).get().getValueAsString());
      assertEquals(0, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyRecordModeOnErrorHandling(
      Class<C> clazz,
      Processor processor,
      OnRecordError onRecordError
  )
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .setOnRecordError(onRecordError)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      Record record1 = RecordCreator.create();
      record1.set(Field.create("Hello"));
      Record record2 = RecordCreator.create();
      record2.set(Field.create("Bye"));
      List<Record> input = Arrays.asList(record1, record2);
      StageRunner.Output output = runner.runProcess(input);
      if (onRecordError == OnRecordError.DISCARD) {
        assertEquals(1, output.getRecords().get("lane").size());
        assertEquals("Bye", output.getRecords().get("lane").get(0).get().getValueAsString());
        assertEquals(0, runner.getErrorRecords().size());
      } else if (onRecordError == OnRecordError.TO_ERROR) {
        assertEquals(1, output.getRecords().get("lane").size());
        assertEquals("Bye", output.getRecords().get("lane").get(0).get().getValueAsString());
        assertEquals(1, runner.getErrorRecords().size());
        assertEquals("Hello", runner.getErrorRecords().get(0).get().getValueAsString());
      }
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyBatchModeOnErrorHandling(
      Class<C> clazz,
      Processor processor,
      OnRecordError onRecordError
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .setOnRecordError(onRecordError)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {

      Record record1 = RecordCreator.create();
      record1.set(Field.create("Hello"));
      Record record2 = RecordCreator.create();
      record2.set(Field.create("Bye"));
      List<Record> input = Arrays.asList(record1, record2);
      runner.runProcess(input);
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyPrimitiveTypesPassthrough(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      List<Field> list = new ArrayList<>();
      list.add(Field.create(true));
      list.add(Field.create('a'));
      list.add(Field.create((byte)1));
      list.add(Field.create((short)2));
      list.add(Field.create(3)); //int
      list.add(Field.create((long)4));
      list.add(Field.create((float)5));
      list.add(Field.create((double)6));
      list.add(Field.createDate(new Date()));
      list.add(Field.create("string"));
      list.add(Field.create(new byte[]{1,2,3}));
      record.set(Field.create(list));
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);
      assertEquals(1, output.getRecords().get("lane").size());
      assertEquals(record.get(), output.getRecords().get("lane").get(0).get());
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyPrimitiveTypesFromScripting(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {

      Record record = RecordCreator.create();
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);
      assertEquals(2, output.getRecords().get("lane").size());

      List<Field> list = new ArrayList<>();
      list.add(Field.create(1)); //int
      list.add(Field.create((long) 5));
      list.add(Field.create(0.5)); //double
      list.add(Field.create(true));
      list.add(Field.create("hello"));
      Field field = Field.create(list);
      assertEquals(field, output.getRecords().get("lane").get(0).get());

      assertEquals(Field.create((List) null), output.getRecords().get("lane").get(1).get());
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyPreserveTypeForNullValue(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    Record record = RecordCreator.create();
    List<Field> list = new LinkedList<>();
    list.add(Field.create(Field.Type.INTEGER, null));
    list.add(Field.create(Field.Type.LONG, null));
    list.add(Field.create(Field.Type.DOUBLE, null));
    list.add(Field.create(Field.Type.FLOAT, null));
    list.add(Field.create(Field.Type.DECIMAL, null));
    list.add(Field.create(Field.Type.BOOLEAN, null));
    list.add(Field.create(Field.Type.STRING, null));
    list.add(Field.create(Field.Type.TIME, null));
    list.add(Field.createDate(null));

    Field field = Field.create(list);
    record.set(field);
    List<Record> input = Collections.singletonList(record);

    try {
      StageRunner.Output output = runner.runProcess(input);
      assertEquals(1, output.getRecords().get("lane").size());
      List<Field> result = output.getRecords().get("lane").get(0).get().getValueAsList();
      for(int i = 0; i < list.size(); i++){
        assertEquals(list.get(i).getType(), result.get(i).getType());
      }
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyPrimitiveTypesFromScriptingJavaScript(
      Class<C> clazz,
      Processor processor
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {

      Record record = RecordCreator.create();
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);
      assertEquals(2, output.getRecords().get("lane").size());

      List<Field> list = new ArrayList<>();
      // JavaScript only defines "Number" as a type which is a 64-bit float (double)
      if (System.getProperty("java.version").startsWith("1.7.")) {
        list.add(Field.create((double) 1)); //double
      }
      // Java8's Nashorn engine however, will respect the original Java type of Integer.
      if (System.getProperty("java.version").startsWith("1.8")) {
        list.add(Field.create(1)); // int
      }
      list.add(Field.create(0.5d));
      list.add(Field.create(true));
      list.add(Field.create("hello"));
      Field field = Field.create(list);
      assertEquals(field, output.getRecords().get("lane").get(0).get());

      assertEquals(Field.create((List) null), output.getRecords().get("lane").get(1).get());
    } finally {
      runner.runDestroy();
    }
  }

  /**
   * Incoming record has type and values, and script evaluator will modify the type
   * as well as actual values. This test checks the output record if types in all
   * fields are modified and have correct type.
   * @param clazz PythonProcessor, JavaScriptProcessor, or GroovyProcessor.
   * @param processor Processor
   * @param <C>
   */
  public static <C extends Processor> void verifyChangedTypeFromScripting(
      Class<C> clazz,
      Processor processor) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("int_long", Field.create(123));  // int
    map.put("long_bool", Field.create(12345L)); // long
    map.put("str_date", Field.create("today")); // String
    map.put("double_decimal", Field.create(123d)); // double
    record.set(Field.create(map));
    StageRunner.Output output;

    try {
      output = runner.runProcess(Collections.singletonList(record));
    } finally {
      runner.runDestroy();
    }

    assertEquals(1, output.getRecords().get("lane").size());
    Map<String, Field> outRec = output.getRecords().get("lane").get(0).get().getValueAsMap();
    if (!clazz.getName().equals(JAVASCRIPT_CLASSNAME)) {
      // JavaScript has ony "Number" type. No int, long, double, decimal types. So skip them.
      assertEquals(Field.Type.LONG, outRec.get("int_long").getType());
      assertEquals(Field.Type.DECIMAL, outRec.get("double_decimal").getType());
      // JavaScript fails this test because Date is Object type.
      assertEquals(Field.Type.DATETIME, outRec.get("str_date").getType());
    }
    assertEquals(Field.Type.BOOLEAN, outRec.get("long_bool").getType());
  }

  public static <C extends Processor> void verifyStateObject(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      Map<String, Field> map = new HashMap<>();
      map.put("count", Field.create(0));
      record.set(Field.create(map));
      List<Record> input = Collections.singletonList(record);
      runner.runProcess(input);
      StageRunner.Output output = runner.runProcess(input);
      assertEquals(1, output.getRecords().get("lane").size());
      assertEquals(2, output.getRecords().get("lane").get(0).get("/count").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyStateObjectJavaScript(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      Map<String, Field> map = new HashMap<>();
      map.put("count", Field.create(0));
      record.set(Field.create(map));
      List<Record> input = Collections.singletonList(record);
      runner.runProcess(input);
      StageRunner.Output output = runner.runProcess(input);
      assertEquals(1, output.getRecords().get("lane").size());
      // JavaScript only has a single number type, which is a double.
      assertEquals(2.0d, output.getRecords().get("lane").get(0).get("/count").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyListMap(Class<C> clazz, Processor processor) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      LinkedHashMap<String, Field> listMap = new LinkedHashMap<>();
      listMap.put("Hello", Field.create(1));

      Record record = RecordCreator.create();
      record.set(Field.createListMap(listMap));
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);

      assertEquals(2, output.getRecords().get("lane").size());
      Record outRec = output.getRecords().get("lane").get(0);
      assertEquals(Field.Type.LIST_MAP, outRec.get().getType());
      // In this case the type passthrough works because the scripting processor didn't modify this field.
      assertEquals(1, outRec.get("/Hello").getValue());
      assertEquals(1, outRec.get("[0]").getValue());
      outRec = output.getRecords().get("lane").get(1);
      assertEquals(Field.Type.LIST_MAP, outRec.get().getType());
      // JavaScript only defines "Number" as a type which is a 64-bit float (double)
      if (System.getProperty("java.version").startsWith("1.7.")) {
        assertEquals(2.0, outRec.get("/Hello").getValue());
        assertEquals(2.0, outRec.get("[0]").getValue());
      }
      // Java8's Nashorn engine however, will respect the original Java type of Integer.
      if (System.getProperty("java.version").startsWith("1.8")) {
        assertEquals(2, outRec.get("/Hello").getValue());
        assertEquals(2, outRec.get("[0]").getValue());
      }
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyMapListMapCreation(Class<C> clazz, Processor processor) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      LinkedHashMap<String, Field> listMap = new LinkedHashMap<>();
      listMap.put("Hello", Field.create(1));

      Record record = RecordCreator.create();
      record.set(Field.create(listMap));
      record.set("/Hello", Field.create(1));
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);

      assertEquals(2, output.getRecords().get("lane").size());
      Record outRec = output.getRecords().get("lane").get(0);
      assertEquals(Field.Type.MAP, outRec.get().getType());
      // In this case the type passthrough works because the scripting processor didn't modify this field.
      assertEquals(1, outRec.get("/Hello").getValue());
      Field f = outRec.get("/Test");
      assertEquals(Field.Type.LIST_MAP, f.getType());
      assertEquals("streamsets", outRec.get("/Test/Key").getValue());
      outRec = output.getRecords().get("lane").get(1);
      assertEquals(Field.Type.LIST_MAP, outRec.get().getType());
      // JavaScript only defines "Number" as a type which is a 64-bit float (double)
      if (System.getProperty("java.version").startsWith("1.7.")) {
        assertEquals(2.0, outRec.get("/Hello").getValue());
        assertEquals(2.0, outRec.get("[0]").getValue());
      }
      // Java8's Nashorn engine however, will respect the original Java type of Integer.
      if (System.getProperty("java.version").startsWith("1.8")) {
        assertEquals(2, outRec.get("/Hello").getValue());
        assertEquals(2, outRec.get("[0]").getValue());
      }
      f = outRec.get("/Test");
      // The field referred to as /Test and [1] should be the same
      assertEquals(f, outRec.get("[1]"));
      assertEquals(Field.Type.MAP, f.getType());
      assertEquals("dpm", outRec.get("/Test/Key").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyEventCreation(Class<C> clazz, Processor processor) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      record.set(Field.create(Collections.<String, Field>emptyMap()));

      StageRunner.Output output = runner.runProcess(Collections.singletonList(record));

      List<EventRecord> events = runner.getEventRecords();
      Assert.assertNotNull(events);
      assertEquals(1, events.size());

      Record event = events.get(0);
      Assert.assertNotNull(event);
      assertTrue(event.has("/a"));
      assertEquals(1, event.get("/a").getValueAsInteger());
      assertTrue(event.has("/b"));
      assertEquals(2, event.get("/b").getValueAsInteger());
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyListMapOrder(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      LinkedHashMap<String, Field> listMap = new LinkedHashMap<>();
      for (int i = 0; i < 20; i++) {
        listMap.put("A" + i, Field.create(1));
      }

      Record record = RecordCreator.create();
      record.set(Field.createListMap(listMap));
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);

      assertEquals(1, output.getRecords().get("lane").size());
      Record outRec = output.getRecords().get("lane").get(0);
      assertEquals(Field.Type.LIST_MAP, outRec.get().getType());

      assertEquals(20, outRec.get("/").getValueAsListMap().size());
      assertEquals(
          new ArrayList<>(listMap.keySet()),
          new ArrayList<>(outRec.get("/").getValueAsListMap().keySet())
      );
    } finally {
      runner.runDestroy();
    }
  }

  /**
   * Test for nested map. It changes the values in nested map and confirm that
   * the types are preserved.
   * @param clazz JythonEvaluator, JavaScripEvaluator or GroovyEvaluator.
   * @param processor Processor
   * @param <C>
   * @throws StageException
   */
  public static <C extends Processor> void verifyNestedMap(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    /*
       This is the setup. Test will change col1-col4 to null, and row2 to null.
       { row1: {
                 col1: true,  // boolean
                 col2: 10L,   // long
                 col3: today's date // Date
                 col4: 0.5d // double
              },
         row2 : {
                 col5: 0.5f  //float
              }
       }
     */
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    Map<String, Field> row1 = new HashMap<>();
    Map<String, Field> row2 = new HashMap<>();
    row1.put("col1", Field.create(true));
    row1.put("col2", Field.create(10L));
    row1.put("col3", Field.createDate(new Date()));
    row1.put("col4", Field.create(0.5d));
    row2.put("col5", Field.create(0.5f));
    map.put("row1", Field.create(row1));
    map.put("row2", Field.create(row2));

    record.set(Field.create(map));
    StageRunner.Output output;

    try {
      output = runner.runProcess(Collections.singletonList(record));
    } finally {
      runner.runDestroy();
    }

    assertEquals(1, output.getRecords().get("lane").size());
    Record outRec = output.getRecords().get("lane").get(0);
    assertEquals(Field.Type.MAP, outRec.get().getType());

    // All of the values in the "row1" map are null, but type should be preserved
    Map<String, Field> row1map = outRec.get().getValueAsMap().get("row1").getValueAsMap();
    for (Map.Entry<String, Field> r1 : row1map.entrySet()){
      Assert.assertNull(r1.getValue().getValue());
      assertEquals(r1.getValue().getType(), row1.get(r1.getKey()).getType());
    }
    // "row2" map is null, but the type should be preserved
    assertEquals(Field.Type.MAP, outRec.get().getValueAsMap().get("row2").getType());
    Assert.assertNull(outRec.get().getValueAsMap().get("row2").getValue());
  }

  public static <C extends Processor> void verifyTypedFieldWithNullValue(
      Class<C> clazz,
      Processor processor,
      Record record
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();

    runner.runInit();
    StageRunner.Output output;
    try{
      output = runner.runProcess(Collections.singletonList(record));
    } finally {
      runner.runDestroy();
    }
    Record outRec = output.getRecords().get("lane").get(0);
    assertEquals(record.get().getValueAsMap().size(), outRec.get().getValueAsMap().size());
    Map<String, Field> outMap = outRec.get().getValueAsMap();
    for(Map.Entry<String, Field> entry : outMap.entrySet()) {
      assertFieldUtil(entry.getKey(), entry.getValue(), null);
    }
  }

  public static <C extends Processor> void verifyNullField(
      Class<C> clazz,
      Processor processor,
      Record record
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();

    runner.runInit();
    StageRunner.Output output;
    try{
      output = runner.runProcess(Collections.singletonList(record));
    } finally {
      runner.runDestroy();
    }
    Record outRec = output.getRecords().get("lane").get(0);
    assertEquals(record.get().getValueAsMap().size(), outRec.get().getValueAsMap().size());
    Map<String, Field> outMap = outRec.get().getValueAsMap();

    List<Field> nullList = new ArrayList<>();
    nullList.add(Field.create("elem1"));
    nullList.add(Field.create("elem2"));

    Map<String, Field> nullMap = new HashMap<>();
    nullMap.put("x", Field.create("X"));
    nullMap.put("y", Field.create("Y"));

    assertFieldUtil("null_int", outMap.get("null_int"), 123);
    assertFieldUtil("null_string", outMap.get("null_string"), "test");
    assertFieldUtil("null_boolean", outMap.get("null_boolean"), true);
    assertFieldUtil("null_list", outMap.get("null_list"), nullList);
    assertFieldUtil("null_map", outMap.get("null_map"),  nullMap);

    assertFieldUtil("null_datetime", outMap.get("null_datetime"), record.get("/null_datetime").getValueAsDatetime());
  }

  public static <C extends Processor> void verifyFileRef(
      Class<C> clazz,
      Processor processor
  ) throws Exception {

    String testDir = "target/" + UUID.randomUUID().toString();
    File testDirectory = new File(testDir);
    testDirectory.deleteOnExit();
    assertTrue(testDirectory.mkdirs());

    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();

    runner.runInit();
    StageRunner.Output output;
    try {
      FileRefTestUtil.writePredefinedTextToFile(testDirectory);

      Record record = RecordCreator.create();
      Field field = FileRefUtil.getWholeFileRecordRootField(
          FileRefTestUtil.getLocalFileRef(testDirectory, false, null, null),
          Files.readAttributes(Paths.get(testDir), "*")
      );
      record.set(field);

      output = runner.runProcess(Collections.singletonList(record));

    } finally {
      runner.runDestroy();
      deleteDirectory(Paths.get(testDir));
    }

    List<Record> records = output.getRecords().get("lane");

    assertEquals(1, records.size());

    Record record = records.get(0);

    assertTrue(record.has("/byte_array"));

    List<Field> bytes = record.get("/byte_array").getValueAsList();

    byte[] byte_array = new byte[bytes.size()];
    for (int i = 0; i < bytes.size(); i++) {
      byte_array[i] = (byte) bytes.get(i).getValueAsInteger();
    }
    assertEquals(FileRefTestUtil.TEXT, new String(byte_array));

    assertTrue(record.has("/fileRef"));
    assertEquals(Field.Type.FILE_REF, record.get("/fileRef").getType());
  }

  private static void deleteDirectory(Path path) throws IOException {
    Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }

    });
  }

  public static <C extends Processor> void verifyCreateRecord(
      Class<C> clazz,
      Processor processor
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("record_value", Field.create("record_value"));
    record.set(Field.create(map));

    runner.runInit();
    StageRunner.Output output;
    try{
      output = runner.runProcess(Collections.singletonList(record));
    } finally {
      runner.runDestroy();
    }
    List<Record> records = output.getRecords().get("lane");
    assertEquals(2, records.size());
    assertEquals("record_value", records.get(0).get("/record_value").getValueAsString());
    assertEquals("record_value", records.get(1).get("/record_value").getValueAsString());
  }

  public static <C extends Processor> void verifyAccessToSdcRecord(
      Class<C> clazz,
      Processor processor
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    Field value = Field.create("value");
    value.setAttribute("attr", "is-here");
    map.put("value", value);
    record.set(Field.create(map));

    runner.runInit();
    StageRunner.Output output;
    try{
      output = runner.runProcess(Collections.singletonList(record));
    } finally {
      runner.runDestroy();
    }
    List<Record> records = output.getRecords().get("lane");
    assertEquals(1, records.size());

    Record outputRecord = records.get(0);

    // Header attribute
    assertEquals("is-here", outputRecord.getHeader().getAttribute("attr"));
  }

  public static <C extends Processor> void verifyRecordHeaderAttribute(
      Class<C> clazz,
      Processor processor,
      Record record
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    // This header should be always removed
    record.getHeader().setAttribute("remove", "please remove me");

    StageRunner.Output output;
    try {
      List<Record> input = Collections.singletonList(record);
      output = runner.runProcess(input);
    } finally {
      runner.runDestroy();
    }
    List<Record> records = output.getRecords().get("lane");
    assertEquals(1, records.size());

    Record.Header outputHeader = records.get(0).getHeader();
    assertEquals(1, outputHeader.getAttributeNames().size());

    final String key = "key1";
    final String value = "value1";
    assertEquals(value, outputHeader.getAttribute(key));

    assertFalse(outputHeader.getAttributeNames().contains("remove"));
  }

  public static <C extends Processor> void verifyInitDestroy(Class<C> clazz, Processor processor) throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    record.set(Field.create(new HashMap<>()));

    runner.runInit();
    StageRunner.Output output;
    try {
      List<Record> input = Collections.singletonList(record);
      output = runner.runProcess(input);
    } finally {
      runner.runDestroy();
    }

    // Validate init method by getting normal record from process with value that was set in init()
    List<Record> records = output.getRecords().get("lane");
    assertEquals(1, records.size());
    assertTrue(records.get(0).has("/initValue"));
    assertEquals("init", records.get(0).get("/initValue").getValueAsString());

    // Validate destroy method by getting event that is generated only there
    List<EventRecord> events = runner.getEventRecords();
    assertEquals(1, events.size());
  }

  public static <C extends Processor> void verifyConstants(Class<C> clazz, Processor processor) throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addConstants(Collections.singletonMap("company", "StreamSets"))
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    record.set(Field.create(new HashMap<>()));

    runner.runInit();
    StageRunner.Output output;
    try {
      List<Record> input = Collections.singletonList(record);
      output = runner.runProcess(input);
    } finally {
      runner.runDestroy();
    }

    // Validate init method by getting normal record from process with value that was set in init()
    List<Record> records = output.getRecords().get("lane");
    assertEquals(1, records.size());
    assertTrue(records.get(0).has("/company"));
    assertEquals("StreamSets", records.get(0).get("/company").getValueAsString());
  }

  public static <C extends Processor> void verifyIsPreview(Class<C> clazz, Processor processor) throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .setPreview(true)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    record.set(Field.create(new HashMap<>()));

    runner.runInit();
    StageRunner.Output output;
    try {
      List<Record> input = Collections.singletonList(record);
      output = runner.runProcess(input);
    } finally {
      runner.runDestroy();
    }

    List<Record> records = output.getRecords().get("lane");
    assertEquals(1, records.size());
    assertTrue(records.get(0).has("/isPreview"));
    assertEquals("true", records.get(0).get("/isPreview").getValueAsString());
  }

  static void assertFieldUtil(String fieldName, Field field, Object obj){
    Field.Type expectedType = null;

    switch(fieldName){
      case "null_boolean":
        expectedType = Field.Type.BOOLEAN;
        break;
      case "null_char":
        expectedType = Field.Type.CHAR;
        break;
      case "null_byte":
        expectedType = Field.Type.BYTE;
        break;
      case "null_short":
        expectedType = Field.Type.SHORT;
        break;
      case "null_int":
        expectedType = Field.Type.INTEGER;
        break;
      case "null_long":
        expectedType = Field.Type.LONG;
        break;
      case "null_float":
        expectedType = Field.Type.FLOAT;
        break;
      case "null_double":
        expectedType = Field.Type.DOUBLE;
        break;
      case "null_date":
        expectedType = Field.Type.DATE;
        break;
      case "null_datetime":
        expectedType = Field.Type.DATETIME;
        break;
      case "null_time":
        expectedType = Field.Type.TIME;
        break;
      case "null_decimal":
        expectedType = Field.Type.DECIMAL;
        break;
      case "null_byteArray":
        expectedType =  Field.Type.BYTE_ARRAY;
        break;
      case "null_string":
        expectedType = Field.Type.STRING;
        break;
      case "null_list":
        expectedType = Field.Type.LIST;
        break;
      case "null_map":
        expectedType = Field.Type.MAP;
        break;
    }
    assertEquals(expectedType, field.getType());
    if(obj == null)
      Assert.assertNull(field.getValue());
    else
      assertEquals(obj, field.getValue());
  }

  public static <C extends Processor> void verifyErrorRecordStopPipeline(
      Class<C> clazz,
      Processor processor
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .addOutputLane("lane")
      .build();

    Record record = RecordCreator.create();
    record.set(Field.create("Not Important"));

    runner.runInit();
    try {
      runner.runProcess(Collections.singletonList(record));
      Assert.fail("Expected exception");
    } catch(Exception e) {
      Assert.assertTrue(e.toString(), e.toString().contains("Script sent record to error"));
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyErrorRecordDiscard(
      Class<C> clazz,
      Processor processor
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
      .setOnRecordError(OnRecordError.DISCARD)
      .addOutputLane("lane")
      .build();

    Record record = RecordCreator.create();
    record.set(Field.create("Not Important"));

    runner.runInit();
    StageRunner.Output output;
    try {
      output = runner.runProcess(Collections.singletonList(record));
    } finally {
      runner.runDestroy();
    }

    Assert.assertEquals(0, output.getRecords().get("lane").size());
  }

  public static <C extends Processor> void verifyErrorRecordErrorSink(
      Class<C> clazz,
      Processor processor
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addOutputLane("lane")
      .build();

    Record record = RecordCreator.create();
    record.set(Field.create("Not Important"));

    runner.runInit();
    StageRunner.Output output;
    try {
      output = runner.runProcess(Collections.singletonList(record));
    } finally {
      runner.runDestroy();
    }

    Assert.assertEquals(0, output.getRecords().get("lane").size());
    Assert.assertEquals(1, runner.getErrorRecords().size());
  }

  public static <C extends Processor> void verifySdcRecord(
      Class<C> clazz,
      Processor processor
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("old", Field.create("old-value"));
    record.set(Field.create(map));

    runner.runInit();
    StageRunner.Output output;
    try{
      output = runner.runProcess(Collections.singletonList(record));
    } finally {
      runner.runDestroy();
    }
    List<Record> records = output.getRecords().get("lane");
    assertEquals(1, records.size());

    Record outputRecord = records.get(0);

    assertTrue(outputRecord.has("/new"));
    assertEquals("new-value", outputRecord.get("/new").getValueAsString());

    assertTrue(outputRecord.has("/old"));
    assertEquals("old-value", outputRecord.get("/old").getValueAsString());
  }

  public static <C extends Processor> void verifyUserParams(
      Class<C> clazz,
      Processor processor
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    record.set(Field.create(map));

    runner.runInit();
    StageRunner.Output output;
    try{
      output = runner.runProcess(Collections.singletonList(record));
    } finally {
      runner.runDestroy();
    }
    List<Record> records = output.getRecords().get("lane");
    assertEquals(1, records.size());

    Record outputRecord = records.get(0);

    assertTrue(outputRecord.has("/user-param-key"));
    assertEquals("user-param-value", outputRecord.get("/user-param-key").getValueAsString());
  }

  @Deprecated
  public static final ImmutableMap<String, String> renames =
      new ImmutableMap.Builder<String, String>()
          .put("error", "sdc.error")
          .put("state", "sdc.state")
          .put("sdcFunctions.isPreview()", "sdc.isPreview()")
          .put("sdcFunctions.createMap(isListMap)", "sdc.createMap(isListMap)")
          .put("sdcFunctions.pipelineParameters()", "sdc.pipelineParameters()")
          .put("NULL_BOOLEAN", "sdc.NULL_BOOLEAN")
          .put("NULL_CHAR", "sdc.NULL_CHAR")
          .put("NULL_BYTE", "sdc.NULL_BYTE")
          .put("NULL_SHORT", "sdc.NULL_SHORT")
          .put("NULL_INTEGER", "sdc.NULL_INTEGER")
          .put("NULL_LONG", "sdc.NULL_LONG")
          .put("NULL_FLOAT", "sdc.NULL_FLOAT")
          .put("NULL_DOUBLE", "sdc.NULL_DOUBLE")
          .put("NULL_DATE", "sdc.NULL_DATE")
          .put("NULL_DATETIME", "sdc.NULL_DATETIME")
          .put("NULL_TIME", "sdc.NULL_TIME")
          .put("NULL_DECIMAL", "sdc.NULL_DECIMAL")
          .put("NULL_BYTE_ARRAY", "sdc.NULL_BYTE_ARRAY")
          .put("NULL_STRING", "sdc.NULL_STRING")
          .put("NULL_LIST", "sdc.NULL_LIST")
          .put("NULL_MAP", "sdc.NULL_MAP")
          // These mappings can't be tested this way
          //.put("sdcFunctions.getFieldNull(record, \"/NULL_CHAR\")", "sdc.getFieldNull(record, \"/sdc.NULL_CHAR\")")
          //.put("sdcFunctions.createRecord(\"id\")", "sdc.createRecord(\"id\")")
          //.put("sdcFunctions.createEvent(\"someType\", 1)", "sdc.createEvent(\"someType\", 1)")
          .build();

  @Deprecated
  public static String writeBindingTestScript(String first, String middle, String last, List<String> vars) {
    StringBuilder builder = new StringBuilder();
    builder.append(first);
    for (String var : vars) {
      builder.append(String.format(middle, var, var));
    }
    builder.append(last);
    return builder.toString();
  }

  @Deprecated
  public static <C extends Processor> void verifyDeprecatedBindings(
      Class<C> clazz, Processor processor ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    record.set(Field.create(map));

    runner.runInit();
    StageRunner.Output output;
    try {
      output = runner.runProcess(Collections.singletonList(record));
    } finally {
      runner.runDestroy();
    }
    List<Record> records = output.getRecords().get("lane");
    assertEquals(1, records.size());
    Record outputRecord = records.get(0);

    String oldBinding = "/";
    String newBinding = "/";
    for (Map.Entry<String, String> entry : renames.entrySet()) {
      try {
        oldBinding = "/" + entry.getKey();
        newBinding = "/" + entry.getValue();
        assertTrue(record.has(oldBinding));
        assertTrue(record.has(newBinding));
        assertEquals(record.get(oldBinding), record.get(newBinding));
      } catch (AssertionError e) {
        System.out.println("Failed on " + entry.toString());
        System.out.println(record.has(oldBinding) + " " + record.get(oldBinding));
        System.out.println(record.has(newBinding) + " " + record.get(newBinding));
        throw e;
      }
    }

  }

  public static void verifyNativeNullRootValue(Class clazz, Processor processor) {
    Record inRec = RecordCreator.create();
    inRec.set(Field.create("original value"));
    List<Record> input = Collections.singletonList(inRec);

    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    StageRunner.Output output;
    try {
      runner.runInit();
      output = runner.runProcess(input);
    } finally {
      runner.runDestroy();
    }

    assertEquals(1, output.getRecords().get("lane").size());
    // a native null or None should come back as a null String field
    Field expectedField = Field.create(Field.Type.STRING, null);
    assertEquals(expectedField, output.getRecords().get("lane").get(0).get());
  }
}
