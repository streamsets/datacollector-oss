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
import java.util.List;
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
      Assert.assertEquals("xxx-xx-etsi", result.get("name").getValue());
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
  public void testCustomMaskWithOtherNonMaskCharacters() throws StageException {
    FieldMaskConfig maskConfig1 = new FieldMaskConfig();
    maskConfig1.fields = ImmutableList.of("/cardno[0]");
    maskConfig1.maskType = MaskType.CUSTOM;
    //digits - 1-4
    maskConfig1.mask = "####aaaaaaaaaaaa";

    FieldMaskConfig maskConfig2 = new FieldMaskConfig();
    maskConfig2.fields = ImmutableList.of("/cardno[1]");
    maskConfig2.maskType = MaskType.CUSTOM;
    maskConfig2.mask = "----####--------";

    FieldMaskConfig maskConfig3 = new FieldMaskConfig();
    maskConfig3.fields = ImmutableList.of("/cardno[2]");
    maskConfig3.maskType = MaskType.CUSTOM;
    maskConfig3.mask = "////////####////";

    FieldMaskConfig maskConfig4 = new FieldMaskConfig();
    maskConfig4.fields = ImmutableList.of("/cardno[3]");
    maskConfig4.maskType = MaskType.CUSTOM;
    maskConfig4.mask = "@@@@@@@@@@@@####";

    char[] maskedChars = new char[] {'a', '-', '/', '@'};

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskDProcessor.class)
        .addConfiguration("fieldMaskConfigs", ImmutableList.of(maskConfig1, maskConfig2, maskConfig3, maskConfig4))
        .addOutputLane("a").build();
    runner.runInit();

    try {
      String cardno = "7122852326437290";
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("cardno", Field.create(
          ImmutableList.of(Field.create(cardno), Field.create(cardno), Field.create(cardno), Field.create(cardno)))
      );
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(1, result.size());

      List<Field> fields = result.get("cardno").getValueAsList();
      Assert.assertEquals(4, fields.size());
      for (int i = 0 ; i < fields.size(); i++) {
        char maskedChar = maskedChars[i];
        String maskedCardNum = fields.get(i).getValueAsString();
        for (int j = 0 ; j < cardno.length(); j++) {
          int boundaryStart = i * 4;
          int boundaryEnd = boundaryStart + 4;
          char expectedChar = (j >= boundaryStart && j < boundaryEnd)? cardno.charAt(j) : maskedChar;
          char actualChar = maskedCardNum.charAt(j);
          Assert.assertEquals(expectedChar, actualChar);
        }
      }
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
      String errorMessage = runner.getErrorRecords().get(0).getHeader().getErrorMessage();
      Assert.assertTrue("Given error message doesn't contain expected substring: " + errorMessage , errorMessage.contains("/name, /age"));
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
      Assert.assertEquals("987-543-10", result.get("phone").getValue());
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

  @Test
  public void testFieldMaskProcessorRegex() throws StageException {

    FieldMaskConfig creditCardMask = new FieldMaskConfig();
    creditCardMask.fields = ImmutableList.of("/creditCard", "/age", "/phone", "/ssn");
    creditCardMask.maskType = MaskType.REGEX;
    creditCardMask.regex = "(.*)(\\d{4})$";
    creditCardMask.mask = null;
    creditCardMask.groupsToShow = "2";

    FieldMaskConfig amexMask = new FieldMaskConfig();
    amexMask.fields = ImmutableList.of("/amex");
    amexMask.maskType = MaskType.REGEX;
    amexMask.regex = "\\d{4}-\\d{6}-(\\d{5})$";
    amexMask.mask = null;
    amexMask.groupsToShow = "1";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskDProcessor.class)
        .addConfiguration("fieldMaskConfigs", ImmutableList.of(creditCardMask, amexMask))
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("creditCard", Field.create("1234-5678-9101-1121"));
      map.put("amex", Field.create("1234-567891-01112"));
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
      Assert.assertTrue(result.size() == 5);
      Assert.assertTrue(result.containsKey("creditCard"));
      Assert.assertEquals("xxxxxxxxxxxxxxx1121", result.get("creditCard").getValue());
      Assert.assertTrue(result.containsKey("amex"));
      Assert.assertEquals("xxxxxxxxxxxx01112", result.get("amex").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals("12", result.get("age").getValue());
      Assert.assertTrue(result.containsKey("ssn"));
      Assert.assertEquals("xxxxxxx6789", result.get("ssn").getValue());
      Assert.assertTrue(result.containsKey("phone"));
      Assert.assertEquals(null, result.get("phone").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = StageException.class)
  public void testFieldMaskProcessorRegexInvalidGroup1() throws StageException {

    FieldMaskConfig creditCardMask = new FieldMaskConfig();
    creditCardMask.fields = ImmutableList.of("/creditCard");
    creditCardMask.maskType = MaskType.REGEX;
    creditCardMask.regex = "(.*)(\\d{4})(-)(\\d{4})$";
    creditCardMask.mask = null;
    creditCardMask.groupsToShow = "3,4,5";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskDProcessor.class)
        .addConfiguration("fieldMaskConfigs", ImmutableList.of(creditCardMask))
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("creditCard", Field.create("1234-5678-9101-1121"));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("creditCard"));
      //Since the group to show '5' is not valid it is ignored, groups 3 and 4 are shown
      Assert.assertEquals("xxxxxxxxxxxxxx-1121", result.get("creditCard").getValue());

    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = StageException.class)
  public void testFieldMaskProcessorRegexInvalidGroup2() throws StageException {

    FieldMaskConfig creditCardMask = new FieldMaskConfig();
    creditCardMask.fields = ImmutableList.of("/creditCard");
    creditCardMask.maskType = MaskType.REGEX;
    creditCardMask.regex = "(.*)(\\d{4})(-)(\\d{4})$";
    creditCardMask.mask = null;
    creditCardMask.groupsToShow = "-1";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskDProcessor.class)
        .addConfiguration("fieldMaskConfigs", ImmutableList.of(creditCardMask))
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("creditCard", Field.create("1234-5678-9101-1121"));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      runner.runProcess(ImmutableList.of(record));

    } finally {
      runner.runDestroy();
    }
  }

}
