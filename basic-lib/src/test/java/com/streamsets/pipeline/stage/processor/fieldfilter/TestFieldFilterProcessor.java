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
package com.streamsets.pipeline.stage.processor.fieldfilter;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

public class TestFieldFilterProcessor {

  /********************************************************/
  /********************   KEEP RELATED   ******************/
  /********************************************************/

  @Test
  public void testKeepSimple() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/name", "/age"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create("b"));
      map.put("streetAddress", Field.create("c"));
      map.put("city", Field.create("d"));
      map.put("state", Field.create("e"));
      map.put("zip", Field.create(94040));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 2);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertTrue(result.containsKey("age"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testKeepNonExistingFiled() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/city"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create("b"));
      map.put("streetAddress", Field.create("c"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 0);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testKeepQuotedFieldPath() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of("/'name with quotes'", "/age"))
        .addConfiguration("filterOperation", FilterOperation.KEEP)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name with quotes", Field.create("a"));
      map.put("age", Field.create("b"));
      map.put("streetAddress", Field.create("c"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 2);
      Assert.assertTrue(result.containsKey("name with quotes"));
      Assert.assertTrue(result.containsKey("age"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNestedKeep1() throws StageException {
    /*
     * In a deep nested field path try to retain the second elements of an array within an array
     */
    Record record = createNestedRecord();

    //Keep only second elements of array within array "/USA[0]/SanFrancisco/noe/streets"
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of(
        "/USA[0]/SanFrancisco/noe/streets[0][1]/name",
        "/USA[0]/SanFrancisco/noe/streets[1][1]/name"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      //Note that since the first element was removed, the second element is the new first element
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertEquals("b", resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString());

      //Note that since the first element was removed, the second element is the new first element
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertEquals("d", resultRecord.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString());

      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name"));
      Assert.assertFalse(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name"));
    } finally {
      runner.runDestroy();
    }

    /*
     * In a deep nested field path try to retain the first elements of an array within an array
     */
    record = createNestedRecord();
    //Keep only first elements of array within array "/USA[0]/SanFrancisco/noe/streets"
    runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of(
        "/USA[0]/SanFrancisco/noe/streets[0][0]/name",
        "/USA[0]/SanFrancisco/noe/streets[1][0]/name"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertEquals("a", resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString());

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertEquals("c", resultRecord.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString());

      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name"));
      Assert.assertFalse(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNestedKeep2() throws StageException {
    /*
     * In a deep nested record try to retain arbitrary paths
     */
    Record record = createNestedRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of(
        "/USA[0]/SanFrancisco/noe/streets[1][1]/name",
        "/USA[1]/SantaMonica/cole/streets[0][1]/name"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertEquals("d", resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString());

      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertEquals("f", resultRecord.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString());

      Assert.assertFalse(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name"));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNestedKeep3() throws StageException {
    /*
     * try to retain the second element from the root array list.
     * Make sure that this turns out to be the first element in the resulting record
     */
    Record record = createNestedRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of(
        "/USA[1]/SantaMonica/cole/streets[0][1]/name"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);

      Assert.assertTrue(resultRecord.has("/USA[0]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertEquals("f", resultRecord.get("/USA[0]/SantaMonica/cole/streets[0][0]/name").getValueAsString());

      Assert.assertFalse(resultRecord.has("/USA[0]/SantaMonica/cole/streets[0][1]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco"));
      Assert.assertFalse(resultRecord.has("/USA[1]"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNestedKeep4() throws StageException {
    Record record = createNestedRecord();
    /*
     * Keep non existing nested path "/USA[0]/SanFrancisco/cole".
     * Only objects upto /USA[0]/SanFrancisco should exist
     */
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of(
        "/USA[0]/SanFrancisco/cole/streets[0][0]/name",
        "/USA[0]/SanFrancisco/cole/streets[1][0]/name"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco"));
      Assert.assertEquals(0, resultRecord.get("/USA[0]/SanFrancisco").getValueAsMap().size());

      Assert.assertFalse(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name"));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNestedKeep5() throws StageException {
    Record record = createNestedRecord();
    /*
     * keep all entries of a list by specifying path just upto the list
     */
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of(
        "/USA[0]/SanFrancisco/folsom/streets[0]"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0]"));
      //Its a list and it is empty. use wild card [*] to preserve the contents of the list
      Assert.assertEquals(2, resultRecord.get("/USA[0]/SanFrancisco/folsom/streets[0]").getValueAsList().size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNestedKeep6() throws StageException {
    Record record = createNestedRecord();
    /*
     * keep all entries of a map by specifying path just upto the map
     */
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of(
        "/USA[0]/SanFrancisco/noe"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe"));
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString(), "a");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString(), "b");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(), "c");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(), "d");
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNestedKeep7() throws StageException {
    /*
     */
    Record record = createNestedRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of(
            "/USA[2]/SantaClara/main/streets[*][*]/name1"))
        .addConfiguration("filterOperation", FilterOperation.KEEP)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertNotNull(resultRecord);
      Assert.assertTrue(resultRecord.has("/USA[0]/SantaClara/main/streets[0][0]/name1"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SantaClara/main/streets[0][1]/name12"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SantaClara/main/streets[1][0]/name13"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SantaClara/main/streets[1][1]/name12AB"));
      Assert.assertEquals("i", resultRecord.get("/USA[0]/SantaClara/main/streets[0][0]/name1").getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }
  @Test
  public void testNestedKeep8() throws StageException {
    /*
     */
    Record record = createNestedRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of(
            "/USA[2]/SantaClara/main/streets[*][*]/name12*"))
        .addConfiguration("filterOperation", FilterOperation.KEEP)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertNotNull(resultRecord);
      Set<String> paths = resultRecord.getEscapedFieldPaths();
      Assert.assertTrue(resultRecord.has("/USA[0]/SantaClara/main/streets[1][1]/name12AB"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SantaClara/main/streets[0][0]/name1"));


    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNestedKeep9() throws StageException {
    /*check for embedded *
     */
    Record record = createNestedRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of(
            "/USA[2]/SantaClara/ma*/streets[*][*]/name1"))
        .addConfiguration("filterOperation", FilterOperation.KEEP)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Record resultRecord = output.getRecords().get("a").get(0);
      Set <String> r = resultRecord.getEscapedFieldPaths();


      Assert.assertNotNull(resultRecord);
      Set<String> paths = resultRecord.getEscapedFieldPaths();
      Assert.assertFalse(resultRecord.has("/USA[0]/SantaClara/main/streets[1][1]/name12AB"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SantaClara/main/streets[0][0]/name1"));


    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNestedKeep10() throws StageException {
    /* check for embedded ? and * at end of field and after /
     */
    Record record = createNestedRecordForRegex();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of(
            "/USA[?]/San*/*/streets[*][*]/name?2"))
        .addConfiguration("filterOperation", FilterOperation.KEEP)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Record resultRecord = output.getRecords().get("a").get(0);

      Assert.assertNotNull(resultRecord);
      Set<String> paths = resultRecord.getEscapedFieldPaths();
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name12"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noel/streets[0][0]/name12"));
      Assert.assertTrue(resultRecord.has("/USA[1]/SantaClara/main/streets[0][1]/name22"));
      Assert.assertFalse(resultRecord.has("/USA[1]/SantaClara/main/streets[0][0]/name21"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name1234"));


    } finally {
      runner.runDestroy();
    }
  }
  @Test
  public void testNestedKeep11() throws StageException {
    /*
     check for regex * at the end of the line
     */
    Record record = createNestedRecordForRegex();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of(
            "/USA[?]/San*/*/streets[*][*]/name2*"))
        .addConfiguration("filterOperation", FilterOperation.KEEP)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Record resultRecord = output.getRecords().get("a").get(0);

      Assert.assertNotNull(resultRecord);
      Set<String> paths = resultRecord.getEscapedFieldPaths();
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name12"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noel/streets[0][0]/name12"));
      Assert.assertTrue(resultRecord.has("/USA[1]/SantaClara/main/streets[0][1]/name22"));
      Assert.assertTrue(resultRecord.has("/USA[1]/SantaClara/main/streets[0][0]/name21"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name1234"));


    } finally {
      runner.runDestroy();
    }
  }

@Test
  public void testWildCardKeep1() throws StageException {
    /*
     * Use wild card in deep nested paths
     */
    Record record = createNestedRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/USA[*]/SanFrancisco/*/streets[*][*]/name"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name"));

      Assert.assertFalse(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWildCardKeep2() throws StageException {
    /*
     * Use wild card in array within array
     */
    Record record = createNestedRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of(
        "/USA[0]/SanFrancisco/noe/streets[0][*]"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0]"));
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString(), "a");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString(), "b");

    } finally {
      runner.runDestroy();
    }

    /*
     * Use wild card in array
     */
    record = createNestedRecord();
    runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of(
        "/USA[0]/SanFrancisco/noe/streets[*]"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets"));
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString(), "a");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString(), "b");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(), "c");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(), "d");

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWildCardKeep3() throws StageException {

    /*
     * Use wild card in map and array
     */
    Record record = createNestedRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/USA[0]/SanFrancisco/*/streets[*][1]/name"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertEquals("b", resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString());
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertEquals("d", resultRecord.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString());
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name"));

      Assert.assertFalse(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name"));
    } finally {
      runner.runDestroy();
    }

    /*
     * Use wild card in map. Make sure the entire tree of elements is preserved
     */
    record = createNestedRecord();
    runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/USA[0]/SanFrancisco/*"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom"));

      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString(), "a");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString(), "b");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(), "c");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(), "d");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/folsom/streets[0][0]/name").getValueAsString(), "g");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/folsom/streets[0][1]/name").getValueAsString(), "h");

      Assert.assertFalse(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testKeepMultiList1() throws StageException {

    Record inputRecord = createMultiListRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
            .addConfiguration("fields", ImmutableList.of("[*][2]"))
            .addConfiguration("filterOperation", FilterOperation.KEEP)
            .addOutputLane("a").build();
    runner.runInit();

    List<Field> fields = new ArrayList<>();
    for (int i = 0; i <= 10; i++) {
      fields.add(i, Field.create(Field.Type.LIST, ImmutableList.of(Field.create(100 * i + 2))));
    }
    Record expectedRecord = RecordCreator.create("s", "s:1");
    expectedRecord.set(Field.create(fields));

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(inputRecord));
      Record resultRecord = output.getRecords().get("a").get(0);
      System.out.println(resultRecord.toString());
      Assert.assertTrue(resultRecord.equals(expectedRecord));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testKeepMultiList2() throws StageException {

    Record inputRecord = createMultiListRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of("[2][*]"))
        .addConfiguration("filterOperation", FilterOperation.KEEP)
        .addOutputLane("a").build();
    runner.runInit();

    List<Field> fields = new ArrayList<>();
    int i = 2;
    fields.add( Field.create( Field.Type.LIST, ImmutableList.of(
            Field.create(100*i+0),
            Field.create(100*i+1),
            Field.create(100*i+2),
            Field.create(100*i+3),
            Field.create(100*i+4),
            Field.create(100*i+5),
            Field.create(100*i+6),
            Field.create(100*i+7),
            Field.create(100*i+8),
            Field.create(100*i+9),
            Field.create(100*i+10)
    )));

    Record expectedRecord = RecordCreator.create("s", "s:1");
    expectedRecord.set(Field.create(fields));

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(inputRecord));
      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.equals(expectedRecord));
    } finally {
      runner.runDestroy();
    }

  }

  /********************************************************/
  /******************   REMOVE RELATED   ******************/
  /********************************************************/

  @Test
  public void testRemoveSimple() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/name", "/age"))
      .addConfiguration("filterOperation", FilterOperation.REMOVE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create("b"));
      map.put("streetAddress", Field.create("c"));
      map.put("city", Field.create("d"));
      map.put("state", Field.create("e"));
      map.put("zip", Field.create(94040));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertTrue(result.containsKey("city"));
      Assert.assertTrue(result.containsKey("state"));
      Assert.assertTrue(result.containsKey("zip"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRemoveNonExistingField() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/city"))
      .addConfiguration("filterOperation", FilterOperation.REMOVE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create("b"));
      map.put("streetAddress", Field.create("c"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 3);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertTrue(result.containsKey("streetAddress"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWildCardRemove1() throws StageException {

    Record record = createNestedRecord();

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/USA[*]/SanFrancisco/*/streets[*][*]/name"))
      .addConfiguration("filterOperation", FilterOperation.REMOVE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name"));

      Assert.assertEquals(resultRecord.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString(), "e");
      Assert.assertEquals(resultRecord.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValueAsString(), "f");
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWildCardRemove2() throws StageException {
    Record record = createNestedRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/USA[0]/SanFrancisco/*/streets[*][1]/name"))
      .addConfiguration("filterOperation", FilterOperation.REMOVE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name"));

      Assert.assertEquals(resultRecord.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString(), "e");
      Assert.assertEquals(resultRecord.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValueAsString(), "f");
    } finally {
      runner.runDestroy();
    }

    record = createNestedRecord();
    runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/USA[0]/SanFrancisco/noe/streets[0][*]"))
      .addConfiguration("filterOperation", FilterOperation.REMOVE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0]"));
      Assert.assertEquals(0, resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0]").getValueAsList().size());

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name"));

      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(), "c");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(), "d");

      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name"));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWildCardRemove3() throws StageException {

    Record record = createNestedRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/USA[*]/SanFrancisco/noe"))
      .addConfiguration("filterOperation", FilterOperation.REMOVE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name"));

      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name"));
    } finally {
      runner.runDestroy();
    }

    record = createNestedRecord();
    runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/USA[*]"))
      .addConfiguration("filterOperation", FilterOperation.REMOVE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertFalse(resultRecord.has("/USA[0]"));
      Assert.assertFalse(resultRecord.has("/USA[1]"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRemoveNull() throws StageException {
    Record record = createNestedRecord(true);
    // Remove fields that match "/USA[*]/SanFrancisco/*/streets[*][1]/name" and their values are null.
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of("/USA[*]/SanFrancisco/*/streets[*][1]/name"))
        .addConfiguration("filterOperation", FilterOperation.REMOVE_NULL)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name")); // Matched && null

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name")); // Matched && non-null
      Assert.assertNotNull(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValue());

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name")); // Matched && null

      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name")); // Unmatched && null
      Assert.assertNull(record.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValue());
    } finally {
      runner.runDestroy();
    }

    record = createNestedRecord(true);
    // "/CANADA" doesn't match any field, so this should be just no-op.
    runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of("/CANADA"))
        .addConfiguration("filterOperation", FilterOperation.REMOVE_NULL)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name")); // Unmatched && null
      Assert.assertNull(record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValue());

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name")); // Unmatched && non-null
      Assert.assertNotNull(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValue());

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name")); // Unmatched && null
      Assert.assertNull(record.get("/USA[0]/SanFrancisco/folsom/streets[0][1]/name").getValue());

      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name")); // Unmatched && null
      Assert.assertNull(record.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRemoveEmptyString() throws StageException {
    Record record = createNestedRecord(false, "");
    // Remove fields that match "/USA[*]/SanFrancisco/*/streets[*][1]/name" and their values are empty string.
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of("/USA[*]/SanFrancisco/*/streets[*][1]/name"))
        .addConfiguration("filterOperation", FilterOperation.REMOVE_EMPTY)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name")); // Matched && empty

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name")); // Matched && non-empty
      Assert.assertNotEquals("", record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValue());

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name")); // Matched && empty

      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name")); // Unmatched && empty
      Assert.assertEquals("", record.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValue());
    } finally {
      runner.runDestroy();
    }

    record = createNestedRecord(false, "");
    // "/CANADA" doesn't match any field, so this should be just no-op.
    runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of("/CANADA"))
        .addConfiguration("filterOperation", FilterOperation.REMOVE_EMPTY)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name")); // Unmatched && empty
      Assert.assertEquals("", record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValue());

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name")); // Unmatched && non-empty
      Assert.assertNotEquals("", record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValue());

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name")); // Unmatched && empty
      Assert.assertEquals("", record.get("/USA[0]/SanFrancisco/folsom/streets[0][1]/name").getValue());

      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name")); // Unmatched && empty
      Assert.assertEquals("", record.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRemoveNullAndEmptyString() throws StageException {
    Record record1 = createNestedRecord(true);
    Record record2 = createNestedRecord(false, "");
    // Remove fields that match "/USA[*]/SanFrancisco/*/streets[*][1]/name" and their values are null or empty string.
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of("/USA[*]/SanFrancisco/*/streets[*][1]/name"))
        .addConfiguration("filterOperation", FilterOperation.REMOVE_NULL_EMPTY)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record1, record2));
      Assert.assertEquals(2, output.getRecords().get("a").size());

      Record resultRecord1 = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord1.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertFalse(resultRecord1.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name")); // Matched && null

      Assert.assertTrue(resultRecord1.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertTrue(resultRecord1.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name")); // Matched && non-null
      Assert.assertNotNull(record1.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValue());

      Assert.assertTrue(resultRecord1.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertFalse(resultRecord1.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name")); // Matched && null

      Assert.assertTrue(resultRecord1.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertTrue(resultRecord1.has("/USA[1]/SantaMonica/cole/streets[0][1]/name")); // Unmatched && null
      Assert.assertNull(record1.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValue());

      Record resultRecord2 = output.getRecords().get("a").get(1);
      Assert.assertTrue(resultRecord2.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertFalse(resultRecord2.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name")); // Matched && empty

      Assert.assertTrue(resultRecord2.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertTrue(resultRecord2.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name")); // Matched && non-empty
      Assert.assertNotEquals("", record2.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValue());

      Assert.assertTrue(resultRecord2.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertFalse(resultRecord2.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name")); // Matched && empty

      Assert.assertTrue(resultRecord2.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertTrue(resultRecord2.has("/USA[1]/SantaMonica/cole/streets[0][1]/name")); // Unmatched && empty
      Assert.assertEquals("", record2.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValue());
    } finally {
      runner.runDestroy();
    }

    record1 = createNestedRecord(true);
    record2 = createNestedRecord(false, "");
    // "/CANADA" doesn't match any field, so this should be just no-op.
    runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of("/CANADA"))
        .addConfiguration("filterOperation", FilterOperation.REMOVE_NULL_EMPTY)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record1, record2));
      Assert.assertEquals(2, output.getRecords().get("a").size());

      Record resultRecord1 = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord1.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertTrue(resultRecord1.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name")); // Unmatched && null
      Assert.assertNull(record1.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValue());

      Assert.assertTrue(resultRecord1.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertTrue(resultRecord1.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name")); // Unmatched && non-null
      Assert.assertNotNull(record1.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValue());

      Assert.assertTrue(resultRecord1.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertTrue(resultRecord1.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name")); // Unmatched && null
      Assert.assertNull(record1.get("/USA[0]/SanFrancisco/folsom/streets[0][1]/name").getValue());

      Assert.assertTrue(resultRecord1.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertTrue(resultRecord1.has("/USA[1]/SantaMonica/cole/streets[0][1]/name")); // Unmatched && null
      Assert.assertNull(record1.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValue());

      Record resultRecord2 = output.getRecords().get("a").get(1);
      Assert.assertTrue(resultRecord2.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertTrue(resultRecord2.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name")); // Unmatched && empty
      Assert.assertEquals("", record2.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValue());

      Assert.assertTrue(resultRecord2.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertTrue(resultRecord2.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name")); // Unmatched && non-empty
      Assert.assertNotEquals("", record2.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValue());

      Assert.assertTrue(resultRecord2.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertTrue(resultRecord2.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name")); // Unmatched && empty
      Assert.assertEquals("", record2.get("/USA[0]/SanFrancisco/folsom/streets[0][1]/name").getValue());

      Assert.assertTrue(resultRecord2.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertTrue(resultRecord2.has("/USA[1]/SantaMonica/cole/streets[0][1]/name")); // Unmatched && empty
      Assert.assertEquals("", record2.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRemoveConstant() throws StageException {
    String constant = "foo";
    Record record = createNestedRecord(false, constant);
    // Remove fields that match "/USA[*]/SanFrancisco/*/streets[*][1]/name" and their values are constant.
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of("/USA[*]/SanFrancisco/*/streets[*][1]/name"))
        .addConfiguration("filterOperation", FilterOperation.REMOVE_CONSTANT)
        .addConfiguration("constant", constant)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name")); // Matched && constant

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name")); // Matched && not constant
      Assert.assertNotEquals(constant, record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValue());

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertFalse(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name")); // Matched && constant

      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name")); // Unmatched && constant
      Assert.assertEquals(constant, record.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValue());
    } finally {
      runner.runDestroy();
    }

    record = createNestedRecord(false, constant);
    // "/CANADA" doesn't match any field, so this should be just no-op.
    runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of("/CANADA"))
        .addConfiguration("filterOperation", FilterOperation.REMOVE_CONSTANT)
        .addConfiguration("constant", constant)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[0][1]/name")); // Unmatched && constant
      Assert.assertEquals(constant, record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValue());

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/noe/streets[1][1]/name")); // Unmatched && not constant
      Assert.assertNotEquals(constant, record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValue());

      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[0]/SanFrancisco/folsom/streets[0][1]/name")); // Unmatched && constant
      Assert.assertEquals(constant, record.get("/USA[0]/SanFrancisco/folsom/streets[0][1]/name").getValue());

      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][0]/name"));
      Assert.assertTrue(resultRecord.has("/USA[1]/SantaMonica/cole/streets[0][1]/name")); // Unmatched && constant
      Assert.assertEquals(constant, record.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRemoveMultiList1() throws StageException {

    Record inputRecord = createMultiListRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of("[*][2]"))
        .addConfiguration("filterOperation", FilterOperation.REMOVE)
        .addOutputLane("a").build();
    runner.runInit();

    List<Field> fields = new ArrayList<>();
    for (int i = 0; i <= 10; i++) {
      fields.add(i, Field.create( Field.Type.LIST, ImmutableList.of(
              Field.create(100*i+0),
              Field.create(100*i+1),
              Field.create(100*i+3),
              Field.create(100*i+4),
              Field.create(100*i+5),
              Field.create(100*i+6),
              Field.create(100*i+7),
              Field.create(100*i+8),
              Field.create(100*i+9),
              Field.create(100*i+10)
      )));
    }
    Record expectedRecord = RecordCreator.create("s", "s:1");
    expectedRecord.set(Field.create(fields));

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(inputRecord));
      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.equals(expectedRecord));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRemoveMultiList2() throws StageException {

    Record inputRecord = createMultiListRecord();
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterDProcessor.class)
        .addConfiguration("fields", ImmutableList.of("[2][*]"))
        .addConfiguration("filterOperation", FilterOperation.REMOVE)
        .addOutputLane("a").build();
    runner.runInit();

    List<Field> fields = new ArrayList<>();
    for (int i = 0; i <= 10; i++) {
      if (i==2) {
        fields.add(i, Field.create( Field.Type.LIST, ImmutableList.of()));
      }
      else {
        fields.add(i, Field.create( Field.Type.LIST, ImmutableList.of(
                Field.create(100*i+0),
                Field.create(100*i+1),
                Field.create(100*i+2),
                Field.create(100*i+3),
                Field.create(100*i+4),
                Field.create(100*i+5),
                Field.create(100*i+6),
                Field.create(100*i+7),
                Field.create(100*i+8),
                Field.create(100*i+9),
                Field.create(100*i+10)
        )));
      }
    }
    Record expectedRecord = RecordCreator.create("s", "s:1");
    expectedRecord.set(Field.create(fields));

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(inputRecord));
      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(resultRecord.equals(expectedRecord));
    } finally {
      runner.runDestroy();
    }

  }

  private Record createNestedRecord() {
    return createNestedRecord(false, null);
  }

  private Record createNestedRecord(boolean includeNulls) {
    return createNestedRecord(includeNulls, null);
  }

  private Record createNestedRecord(boolean includeNulls, String constant) {
    Field name1 = Field.create("a");
    Field name2 = includeNulls ? Field.create(Field.Type.STRING, null) :
        constant != null ? Field.create(constant) : Field.create("b");
    Map<String, Field> nameMap1 = new HashMap<>();
    nameMap1.put("name", name1);
    Map<String, Field> nameMap2 = new HashMap<>();
    nameMap2.put("name", name2);

    Field name3 = Field.create("c");
    Field name4 = Field.create("d");
    Map<String, Field> nameMap3 = new HashMap<>();
    nameMap3.put("name", name3);
    Map<String, Field> nameMap4 = new HashMap<>();
    nameMap4.put("name", name4);

    Field name5 = Field.create("e");
    Field name6 = includeNulls ? Field.create(Field.Type.STRING, null) :
        constant != null ? Field.create(constant) : Field.create("f");
    Map<String, Field> nameMap5 = new HashMap<>();
    nameMap5.put("name", name5);
    Map<String, Field> nameMap6 = new HashMap<>();
    nameMap6.put("name", name6);

    Field name7 = Field.create("g");
    Field name8 = includeNulls ? Field.create(Field.Type.STRING, null) :
        constant != null ? Field.create(constant) : Field.create("h");

    Map<String, Field> nameMap7 = new HashMap<>();
    nameMap7.put("name", name7);
    Map<String, Field> nameMap8 = new HashMap<>();
    nameMap8.put("name", name8);

    Field name9 = Field.create("i");
    Map<String, Field> nameMap9 = new HashMap<>();
    nameMap9.put("name1", name9);

    Field name10 = Field.create("j");
    Map<String, Field> nameMap10 = new HashMap<>();
    nameMap10.put("name12", name10);

    Field name11 = Field.create("k");
    Map<String, Field> nameMap11 = new HashMap<>();
    nameMap11.put("name13", name11);

    Field name12 = Field.create("l");
    Map<String, Field> nameMap12 = new HashMap<>();
    nameMap12.put("name12AB", name12);


    Field first = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap1), Field.create(nameMap2)));
    Field second = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap3), Field.create(nameMap4)));
    Field third = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap5), Field.create(nameMap6)));
    Field fourth = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap7), Field.create(nameMap8)));
    Field fifth = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap9), Field.create(nameMap10)));
    Field sixth = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap11), Field.create(nameMap12)));

    Map<String, Field> noe = new HashMap<>();
    noe.put("streets", Field.create(ImmutableList.of(first, second)));

    Map<String, Field> folsom = new HashMap<>();
    folsom.put("streets", Field.create(ImmutableList.of(fourth)));


    Map<String, Field> cole = new HashMap<>();
    cole.put("streets", Field.create(ImmutableList.of(third)));

    Map<String, Field> main = new HashMap<>();
    main.put("streets", Field.create(ImmutableList.of(fifth, sixth)));


    Map<String, Field> sfArea = new HashMap<>();
    sfArea.put("noe", Field.create(noe));
    sfArea.put("folsom", Field.create(folsom));

    Map<String, Field> utahArea = new HashMap<>();
    utahArea.put("cole", Field.create(cole));

    Map<String, Field> theBayArea = new HashMap<>();
    theBayArea.put("main", Field.create(main));


    Map<String, Field> california = new HashMap<>();
    california.put("SanFrancisco", Field.create(sfArea));

    Map<String, Field> utah = new HashMap<>();
    utah.put("SantaMonica", Field.create(utahArea));

    Map<String, Field> theBay = new HashMap<>();
    theBay.put("SantaClara", Field.create(theBayArea));

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("USA", Field.create(Field.Type.LIST,
      ImmutableList.of(Field.create(california), Field.create(utah), Field.create(theBay))));

    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    //Nested record looks like this:
    if (includeNulls) {
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString(), "a");
      Assert.assertNull(record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValue()); // null
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(), "c");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(), "d");
      Assert.assertEquals(record.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString(), "e");
      Assert.assertNull(record.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValue()); // null
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/folsom/streets[0][0]/name").getValueAsString(), "g");
      Assert.assertNull(record.get("/USA[0]/SanFrancisco/folsom/streets[0][1]/name").getValue()); // null
    } else if (constant != null) {
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString(), "a");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString(), constant);
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(), "c");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(), "d");
      Assert.assertEquals(record.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString(), "e");
      Assert.assertEquals(record.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValueAsString(), constant);
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/folsom/streets[0][0]/name").getValueAsString(), "g");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/folsom/streets[0][1]/name").getValueAsString(), constant);
    } else {
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString(), "a");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString(), "b");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(), "c");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(), "d");
      Assert.assertEquals(record.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString(), "e");
      Assert.assertEquals(record.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValueAsString(), "f");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/folsom/streets[0][0]/name").getValueAsString(), "g");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/folsom/streets[0][1]/name").getValueAsString(), "h");
    }

    Assert.assertNotEquals(record.get("/USA[2]"), null);
    Assert.assertNotEquals(record.get("/USA[2]/SantaClara"), null);
    Assert.assertNotEquals(record.get("/USA[2]/SantaClara/main"), null);
    Assert.assertNotEquals(record.get("/USA[2]/SantaClara/main/streets[0][0]/name1"), null);
    Assert.assertNotEquals(record.get("/USA[2]/SantaClara/main/streets[0][1]"), null);
    Assert.assertNotEquals(record.get("/USA[2]/SantaClara/main/streets[1][0]"), null);
    Assert.assertNotEquals(record.get("/USA[2]/SantaClara/main/streets[1][1]"), null);

    Assert.assertEquals(record.get("/USA[2]/SantaClara/main/streets[0][0]/name1").getValueAsString(), "i");
    Assert.assertEquals(record.get("/USA[2]/SantaClara/main/streets[0][1]/name12").getValueAsString(), "j");
    Assert.assertEquals(record.get("/USA[2]/SantaClara/main/streets[1][0]/name13").getValueAsString(), "k");
    Assert.assertEquals(record.get("/USA[2]/SantaClara/main/streets[1][1]/name12AB").getValueAsString(), "l");

    return record;
  }

  private Record  createNestedRecordForRegex() {

    Field name1 = Field.create("a");
    Field name2 = Field.create("b");
    Field name3 = Field.create("c");
    Field name4 = Field.create("d");
    Field name5 = Field.create("e");
    Field name6 = Field.create("f");
    Field name7 = Field.create("g");
    Field name8 = Field.create("h");


    Map<String, Field> nameMap1 = new HashMap<>();
    nameMap1.put("name1", name1);
    Map<String, Field> nameMap2 = new HashMap<>();
    nameMap2.put("name12", name2);
    Map<String, Field> nameMap3 = new HashMap<>();
    nameMap3.put("name123", name3);
    Map<String, Field> nameMap4 = new HashMap<>();
    nameMap4.put("name1234", name4);
    Map<String, Field> nameMap5 = new HashMap<>();
    nameMap5.put("name12", name5);
    Map<String, Field> nameMap6 = new HashMap<>();
    nameMap6.put("name123", name6);
    Map<String, Field> nameMap7 = new HashMap<>();
    nameMap7.put("name21", name7);
    Map<String, Field> nameMap8 = new HashMap<>();
    nameMap8.put("name22", name8);



    Field first = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap1), Field.create(nameMap2)));
    Field second = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap3), Field.create(nameMap4)));
    Field third = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap5), Field.create(nameMap6)));
    Field fourth = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap7), Field.create(nameMap8)));

    Map<String, Field> noe = new HashMap<>();
    noe.put("streets", Field.create(ImmutableList.of(first, second)));

    Map<String, Field> noel = new HashMap<>();
    noel.put("streets", Field.create(ImmutableList.of(third)));


    Map<String, Field> main = new HashMap<>();
    main.put("streets", Field.create(ImmutableList.of(fourth)));


    Map<String, Field> sfArea = new HashMap<>();
    sfArea.put("noe", Field.create(noe));
    sfArea.put("noel", Field.create(noel));


    Map<String, Field> theBayArea = new HashMap<>();
    theBayArea.put("main", Field.create(main));

    Map<String, Field> california = new HashMap<>();
    california.put("SanFrancisco", Field.create(sfArea));

    Map<String, Field> theBay = new HashMap<>();
    theBay.put("SantaClara", Field.create(theBayArea));

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("USA", Field.create(Field.Type.LIST,
        ImmutableList.of(Field.create(california), Field.create(theBay))));

    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name1").getValueAsString(), "a");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name12").getValueAsString(), "b");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name123").getValueAsString(), "c");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name1234").getValueAsString(), "d");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noel/streets[0][0]/name12").getValueAsString(), "e");
      Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noel/streets[0][1]/name123").getValueAsString(), "f");

      Assert.assertNotEquals(record.get("/USA[1]"), null);
      Assert.assertNotEquals(record.get("/USA[1]/SantaClara"), null);
      Assert.assertNotEquals(record.get("/USA[1]/SantaClara/main"), null);
      Assert.assertNotEquals(record.get("/USA[1]/SantaClara/main/streets[0][0]/name21"), null);
      Assert.assertNotEquals(record.get("/USA[1]/SantaClara/main/streets[0][1]/name22"), null);
      Assert.assertEquals(record.get("/USA[1]/SantaClara/main/streets[0][0]/name21").getValueAsString(), "g");
      Assert.assertEquals(record.get("/USA[1]/SantaClara/main/streets[0][1]/name22").getValueAsString(), "h");

    return record;
  }

  private Record createMultiListRecord() {

    List<Field> fields = new ArrayList<>();
    for (int i = 0; i <= 10; i++) {
      fields.add(i, Field.create( Field.Type.LIST, ImmutableList.of(
              Field.create(100*i+0),
              Field.create(100*i+1),
              Field.create(100*i+2),
              Field.create(100*i+3),
              Field.create(100*i+4),
              Field.create(100*i+5),
              Field.create(100*i+6),
              Field.create(100*i+7),
              Field.create(100*i+8),
              Field.create(100*i+9),
              Field.create(100*i+10)
      )));
    }
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(fields));
    return record;
  }


}
