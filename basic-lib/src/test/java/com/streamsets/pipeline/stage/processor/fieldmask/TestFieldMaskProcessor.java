/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldmask;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
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

public class TestFieldMaskProcessor {

  @Test
  public void testFieldMaskProcessorVariableLength() throws StageException {

    FieldMaskConfig nameMaskConfig = new FieldMaskConfig();
    nameMaskConfig.fields = ImmutableList.of("/name", "/age", "/ssn", "/phone");
    nameMaskConfig.maskType = MaskType.VARIABLE_LENGTH;
    nameMaskConfig.mask = null;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskDProcessor.class)
      .addConfiguration("fieldMaskConfigs", ImmutableList.of(nameMaskConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsetsinc"));
      map.put("age", Field.create("12"));
      map.put("ssn", Field.create("123-45-6789"));
      map.put("phone", Field.create(Field.Type.STRING, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("xxxxxxxxxxxxx", result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals("xx", result.get("age").getValue());
      Assert.assertTrue(result.containsKey("ssn"));
      Assert.assertEquals("xxxxxxxxxxx", result.get("ssn").getValue());
      Assert.assertTrue(result.containsKey("phone"));
      Assert.assertEquals(null, result.get("phone").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldMaskProcessorFixedLength() throws StageException {

    FieldMaskConfig ageMaskConfig = new FieldMaskConfig();
    ageMaskConfig.fields = ImmutableList.of("/name", "/age", "/ssn", "/phone");
    ageMaskConfig.maskType = MaskType.FIXED_LENGTH;
    ageMaskConfig.mask = null;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskDProcessor.class)
      .addConfiguration("fieldMaskConfigs", ImmutableList.of(ageMaskConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsetsinc"));
      map.put("age", Field.create("12"));
      map.put("ssn", Field.create("123-45-6789"));
      map.put("phone", Field.create(Field.Type.STRING, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("xxxxxxxxxx", result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals("xxxxxxxxxx", result.get("age").getValue());
      Assert.assertTrue(result.containsKey("ssn"));
      Assert.assertEquals("xxxxxxxxxx", result.get("ssn").getValue());
      Assert.assertTrue(result.containsKey("phone"));
      Assert.assertEquals(null, result.get("phone").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldMaskProcessorFormatPreserving() throws StageException {

    FieldMaskConfig formatPreserveMask = new FieldMaskConfig();
    formatPreserveMask.fields = ImmutableList.of("/name", "/age", "/ssn", "/phone");
    formatPreserveMask.maskType = MaskType.CUSTOM;
    formatPreserveMask.mask = "xxx-xx-####";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskDProcessor.class)
      .addConfiguration("fieldMaskConfigs", ImmutableList.of(formatPreserveMask))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsetsinc"));
      map.put("age", Field.create("12"));
      map.put("ssn", Field.create("123-45-6789"));
      map.put("phone", Field.create(Field.Type.STRING, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("xxx-xx-mset", result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals("xx", result.get("age").getValue());
      Assert.assertTrue(result.containsKey("ssn"));
      Assert.assertEquals("xxx-xx-6789", result.get("ssn").getValue());
      Assert.assertTrue(result.containsKey("phone"));
      Assert.assertEquals(null, result.get("phone").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldMaskProcessorNonString() throws StageException {

    FieldMaskConfig formatPreserveMask = new FieldMaskConfig();
    formatPreserveMask.fields = ImmutableList.of("/name", "/age");
    formatPreserveMask.maskType = MaskType.CUSTOM;
    formatPreserveMask.mask = "xxx-xx-####";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("fieldMaskConfigs", ImmutableList.of(formatPreserveMask))
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(12345));
      map.put("age", Field.create(123.56));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, output.getRecords().get("a").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());
      Assert.assertEquals(Errors.MASK_00.name(), runner.getErrorRecords().get(0).getHeader().getErrorCode());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldMaskProcessorMultipleFormats() throws StageException {

    FieldMaskConfig nameMaskConfig = new FieldMaskConfig();
    nameMaskConfig.fields = ImmutableList.of("/name");
    nameMaskConfig.maskType = MaskType.VARIABLE_LENGTH;
    nameMaskConfig.mask = null;

    FieldMaskConfig ageMaskConfig = new FieldMaskConfig();
    ageMaskConfig.fields = ImmutableList.of("/age");
    ageMaskConfig.maskType = MaskType.FIXED_LENGTH;
    ageMaskConfig.mask = null;

    FieldMaskConfig ssnMaskConfig = new FieldMaskConfig();
    ssnMaskConfig.fields = ImmutableList.of("/ssn");
    ssnMaskConfig.maskType = MaskType.CUSTOM;
    ssnMaskConfig.mask = "xxx-xx-####";

    FieldMaskConfig phoneMaskConfig = new FieldMaskConfig();
    phoneMaskConfig.fields = ImmutableList.of("/phone");
    phoneMaskConfig.maskType = MaskType.CUSTOM;
    phoneMaskConfig.mask = "###-###-####";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskDProcessor.class)
      .addConfiguration("fieldMaskConfigs", ImmutableList.of(nameMaskConfig, ageMaskConfig, ssnMaskConfig, phoneMaskConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsetsinc"));
      map.put("age", Field.create("12"));
      map.put("ssn", Field.create("123-45-6789"));
      map.put("phone", Field.create("9876543210"));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("xxxxxxxxxxxxx", result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals("xxxxxxxxxx", result.get("age").getValue());
      Assert.assertTrue(result.containsKey("ssn"));
      Assert.assertEquals("xxx-xx-6789", result.get("ssn").getValue());
      Assert.assertTrue(result.containsKey("phone"));
      Assert.assertEquals("987-654-3210", result.get("phone").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldMaskProcessorFiledDoesNotExist() throws StageException {

    FieldMaskConfig nameMaskConfig = new FieldMaskConfig();
    nameMaskConfig.fields = ImmutableList.of("/name");
    nameMaskConfig.maskType = MaskType.VARIABLE_LENGTH;
    nameMaskConfig.mask = null;

ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskDProcessor.class)
      .addConfiguration("fieldMaskConfigs", ImmutableList.of(nameMaskConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create("12"));
      map.put("ssn", Field.create("123-45-6789"));
      map.put("phone", Field.create("9876543210"));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 3);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals("12", result.get("age").getValue());
      Assert.assertTrue(result.containsKey("ssn"));
      Assert.assertEquals("123-45-6789", result.get("ssn").getValue());
      Assert.assertTrue(result.containsKey("phone"));
      Assert.assertEquals("9876543210", result.get("phone").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWildCardMask() throws StageException {

    Field name1 = Field.create("jon");
    Field name2 = Field.create("natty");
    Map<String, Field> nameMap1 = new HashMap<>();
    nameMap1.put("name", name1);
    Map<String, Field> nameMap2 = new HashMap<>();
    nameMap2.put("name", name2);

    Field name3 = Field.create("adam");
    Field name4 = Field.create("hari");
    Map<String, Field> nameMap3 = new HashMap<>();
    nameMap3.put("name", name3);
    Map<String, Field> nameMap4 = new HashMap<>();
    nameMap4.put("name", name4);

    Field name5 = Field.create("madhu");
    Field name6 = Field.create("girish");
    Map<String, Field> nameMap5 = new HashMap<>();
    nameMap5.put("name", name5);
    Map<String, Field> nameMap6 = new HashMap<>();
    nameMap6.put("name", name6);

    Field first = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap1), Field.create(nameMap2)));
    Field second = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap3), Field.create(nameMap4)));
    Field third = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap5), Field.create(nameMap6)));

    Map<String, Field> noe = new HashMap<>();
    noe.put("streets", Field.create(ImmutableList.of(first, second)));

    Map<String, Field> cole = new HashMap<>();
    cole.put("streets", Field.create(ImmutableList.of(third)));


    Map<String, Field> sfArea = new HashMap<>();
    sfArea.put("noe", Field.create(noe));

    Map<String, Field> utahArea = new HashMap<>();
    utahArea.put("cole", Field.create(cole));


    Map<String, Field> california = new HashMap<>();
    california.put("SanFrancisco", Field.create(sfArea));

    Map<String, Field> utah = new HashMap<>();
    utah.put("SantaMonica", Field.create(utahArea));

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("USA", Field.create(Field.Type.LIST,
      ImmutableList.of(Field.create(california), Field.create(utah))));

    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString(), "jon");
    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString(), "natty");
    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(), "adam");
    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(), "hari");
    Assert.assertEquals(record.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString(), "madhu");
    Assert.assertEquals(record.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValueAsString(), "girish");

    /* All the field Paths in the record are
        /USA
        /USA[0]
        /USA[0]/SantaMonica
        /USA[0]/SantaMonica/noe
        /USA[0]/SantaMonica/noe/streets
        /USA[0]/SantaMonica/noe/streets[0]
        /USA[0]/SantaMonica/noe/streets[0][0]
        /USA[0]/SantaMonica/noe/streets[0][0]/name
        /USA[0]/SantaMonica/noe/streets[0][1]
        /USA[0]/SantaMonica/noe/streets[0][1]/name
        /USA[0]/SantaMonica/noe/streets[1]
        /USA[0]/SantaMonica/noe/streets[1][0]
        /USA[0]/SantaMonica/noe/streets[1][0]/name
        /USA[0]/SantaMonica/noe/streets[1][1]
        /USA[0]/SantaMonica/noe/streets[1][1]/name
        /USA[1]
        /USA[1]/SantaMonica
        /USA[1]/SantaMonica/cole
        /USA[1]/SantaMonica/cole/streets
        /USA[1]/SantaMonica/cole/streets[0]
        /USA[1]/SantaMonica/cole/streets[0][0]
        /USA[1]/SantaMonica/cole/streets[0][0]/name
        /USA[1]/SantaMonica/cole/streets[0][1]
        /USA[1]/SantaMonica/cole/streets[0][1]/name
      */

    FieldMaskConfig nameMaskConfig = new FieldMaskConfig();
    nameMaskConfig.fields = ImmutableList.of("/USA[*]/SanFrancisco/*/streets[*][*]/name");
    nameMaskConfig.maskType = MaskType.FIXED_LENGTH;
    nameMaskConfig.mask = null;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskDProcessor.class)
      .addConfiguration("fieldMaskConfigs", ImmutableList.of(nameMaskConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertEquals("xxxxxxxxxx",
        resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString());
      Assert.assertEquals("xxxxxxxxxx",
        resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString());

      Assert.assertEquals("xxxxxxxxxx",
        resultRecord.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString());
      Assert.assertEquals("xxxxxxxxxx",
        resultRecord.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString());

      Assert.assertEquals(resultRecord.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString(),
        "madhu");
      Assert.assertEquals(resultRecord.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValueAsString(),
        "girish");

    } finally {
      runner.runDestroy();
    }
  }
}
