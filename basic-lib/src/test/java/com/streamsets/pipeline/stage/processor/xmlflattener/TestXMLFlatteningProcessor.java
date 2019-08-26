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
package com.streamsets.pipeline.stage.processor.xmlflattener;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.lib.util.CommonError.CMN_0100;

public class TestXMLFlatteningProcessor {

  private static final Class dProcessorClass = XMLFlatteningDProcessor.class;
  private Processor processor;
  private final String ORIGINAL = "/original";

  private Record createRecord(String xml) {
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    record.set(Field.create(map));
    record.set(ORIGINAL, Field.create(xml));
    return record;
  }

  private String getXML(String id) {
    return "<contact type=\"person\"><name type=\"maiden\" xmlns=\"http://blah.com/blah.xml\">NAME" + id + "</name>" +
        "<phone>(111)111-1111" + id + "</phone><phone>(222)222-2222" + id + "</phone></contact>";
  }

  private String getXMLWithWhitespace(String id) {
    return "<contact type=\"person\">        <name type=\"maiden\" xmlns=\"http://blah.com/blah.xml\">NAME" + id + "</name>\n" +
        "\t<phone>(111)111-1111" + id + "</phone>\n\t<phone>(222)222-2222" + id + "</phone>\n</contact>";
  }

  private String getInvalidXML() {
    return "<contact><name type=\"maiden\" xmlns=\"http://blah.com/blah.xml\">NAME</name>" +
        "<phone>(111)111-1111</phone><phone>(222)222-2222</phone></contact>" +
        "<contact><name type=\"maiden\" xmlns=\"http://blah.com/blah.xml\">NAME</name>" +
        "<phone>(111)111-1111</phone><phone>(222)222-2222</phone></contact>";
  }

  private LinkedHashMap<String, Field> createExpectedRecord(
    String prefix,
    String id,
    String offset,
    String delimStr,
    String attrStr,
    String outputField,
    boolean addAttrs,
    boolean addNS,
    boolean keepFields
  ) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    String baseName = prefix + "contact" + offset + delimStr;

    if (addAttrs) {
      fields.put("contact" + attrStr +"type", Field.create("person"));
      fields.put(baseName + "name" + attrStr + "type", Field.create("maiden"));
    }

    if (addNS) {
      fields.put(baseName + "name" + attrStr + "xmlns", Field.create("http://blah.com/blah.xml"));
    }
    fields.put(baseName + "name", Field.create("NAME" + id));
    fields.put(baseName + "phone(0)", Field.create("(111)111-1111" + id));
    fields.put(baseName + "phone(1)", Field.create("(222)222-2222" + id));

    if(!StringUtils.isEmpty(outputField) && keepFields) {
      LinkedHashMap<String, Field> newRoot = new LinkedHashMap<>();
      newRoot.put(outputField, Field.create(Field.Type.MAP, fields));
      fields = newRoot;
    }

    return fields;
  }

  @Test
  public void testSingleRecordNoAttrsNoNS() throws Exception {
    String xml = getXML("");
    Record expected = RecordCreator.create();
    expected.set(Field.create(createExpectedRecord("", "", "", ".", "#", "", false, false, false)));
    doTest(xml, "contact", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, false, false);
  }

  @Test
  public void testSingleRecordNoAttrsNoNSWithWhiteSpace() throws Exception {
    String xml = getXMLWithWhitespace("");
    Record expected = RecordCreator.create();
    expected.set(Field.create(createExpectedRecord("", "", "", ".", "#", "", false, false, false)));
    doTest(xml, "contact", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, false, false);
  }

  @Test
  public void testSingleRecordAttrsNoNS() throws Exception {
    String xml = getXML("");
    Record expected = RecordCreator.create();
    expected.set(Field.create(createExpectedRecord("", "", "", ".", "#", "", true, false, false)));
    doTest(xml, "contact", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD, false, true, false, false);
  }

  @Test
  public void testSingleRecordNoAttrsNS() throws Exception {
    String xml = getXML("");
    Record expected = RecordCreator.create();
    expected.set(Field.create(createExpectedRecord("", "", "", ".", "#", "", false, true, false)));
    doTest(xml, "contact", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD, true, false, false, false);
  }

  @Test
  public void testSingleRecordAttrsNS() throws Exception {
    String xml = getXML("");
    Record expected = RecordCreator.create();
    expected.set(Field.create(createExpectedRecord("", "", "", "_", ".", "", true, true, false)));
    doTest(xml, "contact", "_", ".", "", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD,
        false, false, false, false);
  }

  @Test
  public void testSingleRecordAttrsNSWithWhitespace() throws Exception {
    String xml = getXMLWithWhitespace("");
    Record expected = RecordCreator.create();
    expected.set(Field.create(createExpectedRecord("", "", "", "_", ".", "", true, true, false)));
    doTest(xml, "contact", "_", ".", "", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD,
        false, false, false, false);
  }

  @Test
  public void testOutputField() throws Exception {
    String xml = getXML("");
    Record expected = RecordCreator.create();
    expected.set(Field.create(createExpectedRecord("", "", "", "_", ".", "output", true, true, true)));
    doTest(xml, "contact", "_", ".", "output", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD, false, false, true, false);
  }

  @Test
  public void testOutputFieldWithKeepFieldsFalse() throws Exception {
    String xml = getXML("");
    Record expected = RecordCreator.create();
    expected.set(Field.create(createExpectedRecord("", "", "", "_", ".", "output", true, true, false)));
    doTest(xml, "contact", "_", ".", "output", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD, false, false, false, false);
  }

  @Test
  public void testMultipleRecords() throws Exception {
    String xml = "<contacts>" + getXML("0") + getXML("1") + "</contacts>";
    Record expected1 = RecordCreator.create();
    expected1.set(Field.create(createExpectedRecord("", "0", "", ".", "#", "", false, false, false)));
    Record expected2 = RecordCreator.create();
    expected2.set(Field.create(createExpectedRecord("", "1", "", ".", "#", "", false, false, false)));
    List<Record> expected = ImmutableList.of(expected1, expected2);
    doTest(xml, "contact", expected, Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, false, false);
  }

  @Test
  public void testMultipleRecordsWhitespace() throws Exception {
    String xml = "<contacts>" + getXMLWithWhitespace("0") + getXMLWithWhitespace("1") + "</contacts>";
    Record expected1 = RecordCreator.create();
    expected1.set(Field.create(createExpectedRecord("", "0", "", ".", "#", "", false, false, false)));
    Record expected2 = RecordCreator.create();
    expected2.set(Field.create(createExpectedRecord("", "1", "", ".", "#", "", false, false, false)));
    List<Record> expected = ImmutableList.of(expected1, expected2);
    doTest(xml, "contact", expected, Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, false, false);
  }

  @Test
  public void testMultipleRecordsNoDelimiter() throws Exception {
    String xml = "<contacts>" + getXML("0") + getXML("1") + "</contacts>";
    Record expected = RecordCreator.create();
    Map<String, Field> allFields = createExpectedRecord("contacts.", "0", "(0)", ".", "#", "", false, false, false);
    allFields.putAll(createExpectedRecord("contacts.", "1", "(1)", ".", "#", "", false, false, false));
    expected.set(Field.create(allFields));
    doTest(xml, "", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, false, false);
  }

  @Test
  public void testMultipleRecordsNoDelimiterWithWhitespace() throws Exception {
    String xml = "<contacts>" + getXMLWithWhitespace("0") + getXMLWithWhitespace("1") + "</contacts>";
    Record expected = RecordCreator.create();
    Map<String, Field> allFields = createExpectedRecord("contacts.", "0", "(0)", ".", "#", "", false, false, false);
    allFields.putAll(createExpectedRecord("contacts.", "1", "(1)", ".", "#", "", false, false, false));
    expected.set(Field.create(allFields));
    doTest(xml, "", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, false, false);
  }

  @Test
  public void testMultipleRecordsNoDelimiterWithAndWithoutWhitespace() throws Exception {
    String xml = "<contacts>" + getXMLWithWhitespace("0") + getXML("1") + "</contacts>";
    Record expected = RecordCreator.create();
    Map<String, Field> allFields = createExpectedRecord("contacts.", "0", "(0)", ".", "#", "", false, false, false);
    allFields.putAll(createExpectedRecord("contacts.", "1", "(1)", ".", "#", "", false, false, false));
    expected.set(Field.create(allFields));
    doTest(xml, "", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, false, false);
  }

  @Test
  public void testWhitespaceAfterProlog() throws Exception {
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>            <note><to>Tove</to>   <from>Jani</from>" +
        "<heading>Reminder</heading>    <body>Don't forget me this weekend!</body></note>";
    Record r = RecordCreator.create();
    r.set(Field.create(new HashMap<String, Field>()));
    r.set(ORIGINAL, Field.create(xml));
    Record expected = RecordCreator.create();
    expected.set(Field.create(new HashMap<String, Field>()));
    expected.set("/note.to", Field.create("Tove"));
    expected.set("/note.from", Field.create("Jani"));
    expected.set("/note.heading", Field.create("Reminder"));
    expected.set("/note.body", Field.create("Don't forget me this weekend!"));
    processor = new XMLFlatteningProcessor(ORIGINAL,false, false, "", null, ".", "#", true, true);
    ProcessorRunner runner = new ProcessorRunner.Builder(dProcessorClass, processor)
        .addOutputLane("xml").setOnRecordError(OnRecordError.DISCARD).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r));
    List<Record> op = output.getRecords().get("xml");
    Assert.assertEquals(1, op.size());
    Assert.assertEquals(expected.get().getValueAsMap(), op.get(0).get().getValueAsMap());
  }


  @Test
  public void testInvalidRecordDiscard() throws Exception {
    doTestInvalidRecord(OnRecordError.DISCARD, false);
  }

  @Test
  public void testInvalidConfig() throws Exception {
    processor = new XMLFlatteningProcessor(ORIGINAL,false, false, "", "contact", "]", "[", true, true);
    ProcessorRunner runner = new ProcessorRunner.Builder(dProcessorClass, processor)
        .addOutputLane("xml").setOnRecordError(OnRecordError.DISCARD).build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(2, issues.size());
  }

  @Test
  public void testInvalidRecordToError() throws Exception {
    doTestInvalidRecord(OnRecordError.TO_ERROR, false);
  }

  @Test (expected = StageException.class)
  public void testInvalidRecordStopPiperline() throws Exception {
    doTestInvalidRecord(OnRecordError.STOP_PIPELINE, false);
  }

  @Test
  public void testInvalidTypeDiscard() throws Exception {
    doTestInvalidRecord(OnRecordError.DISCARD, true);
  }

  @Test
  public void testInvalidTypeToError() throws Exception {
    doTestInvalidRecord(OnRecordError.TO_ERROR, true);
  }

  @Test (expected = StageException.class)
  public void testInvalidTypeStopPiperline() throws Exception {
    doTestInvalidRecord(OnRecordError.STOP_PIPELINE, true);
  }

  private void doTestInvalidRecord(OnRecordError onRecordError, boolean nonString) throws Exception{
    processor = new XMLFlatteningProcessor(ORIGINAL, false, false, "", "<contact>", ".", "#", true, true);
    ProcessorRunner runner = new ProcessorRunner.Builder(dProcessorClass, processor)
        .addOutputLane("xml").setOnRecordError(onRecordError).build();
    runner.runInit();
    Record record = null;
    if (nonString) {
      record = RecordCreator.create();
      record.set(Field.create(new HashMap<String, Field>()));
      record.set(ORIGINAL, Field.create(1000));
    } else {
      record = createRecord(getInvalidXML());
    }

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    if (onRecordError == OnRecordError.DISCARD) {
      Assert.assertTrue(output.getRecords().get("xml").isEmpty());
    } else if (onRecordError == OnRecordError.TO_ERROR) {
      Assert.assertEquals(1, runner.getErrorRecords().size());
      Assert.assertTrue(runner.getErrorRecords().get(0).get().equals(record.get()));
    }
  }

  @Test
  public void testMultipleRecordsKeepOriginalFields() throws Exception {
    String xml = "<contacts>" + getXML("0") + getXML("1") + "</contacts>";
    Record expected1 = RecordCreator.create();
    expected1.set(Field.createListMap(createExpectedRecord("", "0", "", ".", "#", "", false, false, true)));
    expected1.set("/contact.name", Field.create("streamsets"));
    Record expected2 = RecordCreator.create();
    expected2.set(Field.create(createExpectedRecord("", "1", "", ".", "#", "", false, false, true)));
    expected2.set("/contact.name", Field.create("streamsets"));
    List<Record> expected = ImmutableList.of(expected1, expected2);
    doTest(xml, "contact", expected, Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, true, false);
  }

  @Test
  public void testMultipleRecordsKeepOriginalFieldsOverWrite() throws Exception {
    String xml = "<contacts>" + getXML("0") + getXML("1") + "</contacts>";
    Record expected1 = RecordCreator.create();
    expected1.set(Field.create(createExpectedRecord("", "0", "", ".", "#", "", false, false, true)));
    expected1.set("/contact.name", Field.create("streamsets"));
    Record expected2 = RecordCreator.create();
    expected2.set(Field.create(createExpectedRecord("", "1", "", ".", "#", "", false, false, true)));
    List<Record> expected = ImmutableList.of(expected1, expected2);
    doTest(xml, "contact", expected, Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, true, true);
  }

  @Test
  public void testNonMap() throws Exception {
    Record expected1 = RecordCreator.create();
    expected1.set(Field.create(new ArrayList<Field>()));
    processor = new XMLFlatteningProcessor(ORIGINAL, true, false, "", "", "", "", false, false);
    ProcessorRunner runner = new ProcessorRunner.Builder(dProcessorClass, processor)
        .addOutputLane("xml").setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    try {
      runner.runInit();
      runner.runProcess(ImmutableList.of(expected1));
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertEquals(CMN_0100, ex.getErrorCode());
    }
  }

  @Test
  public void testSourceIdPostFixWithoutRecordDelimiter() throws Exception {
    Record record = RecordCreator.create("", "s:s1");

    String xml = "<contacts>" + getXML("0") + getXML("1") + "</contacts>";

    Map<String, Field> rootMap = new HashMap<>();
    rootMap.put("text", Field.create(xml));
    record.set(Field.create(rootMap));

    processor = new XMLFlatteningProcessor("/text", true, false, "", "contact", "", "", false, false);
    ProcessorRunner runner = new ProcessorRunner.Builder(dProcessorClass, processor)
        .addOutputLane("xml").setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      List<Record> records = output.getRecords().get("xml");
      Assert.assertEquals(2, records.size());

      Record record1 = records.get(0);
      Assert.assertEquals(record.getHeader().getSourceId() + "_" + 1, record1.getHeader().getSourceId());

      Record record2 = records.get(1);
      Assert.assertEquals(record.getHeader().getSourceId() + "_" + 2, record2.getHeader().getSourceId());

    } finally {
      runner.runDestroy();
    }

  }

  @Test
  public void testSourceIdPostFixWithRecordDelimiter() throws Exception {
    Record record1 = RecordCreator.create("", "s:s1");
    Map<String, Field> rootMap = new HashMap<>();
    rootMap.put("text", Field.create("<tag></tag>"));
    record1.set(Field.create(rootMap));

    processor = new XMLFlatteningProcessor("/text", true, false, "", "", "", "", false, false);
    ProcessorRunner runner = new ProcessorRunner.Builder(dProcessorClass, processor)
        .addOutputLane("xml").setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record1));
      String expectedSourceId = record1.getHeader().getSourceId() + "_" + 1;
      List<Record> records = output.getRecords().get("xml");
      Assert.assertEquals(1, records.size());
      record1 = records.get(0);
      Assert.assertEquals(expectedSourceId, record1.getHeader().getSourceId());

    } finally {
      runner.runDestroy();
    }
  }

  private void doTest(String xml, String delimiter, List<Record> expected, List<Record> error, OnRecordError onRecordError,
                      boolean ignoreAttrs, boolean ignoreNS, boolean keepFields, boolean newFieldOverwrites) throws Exception {
    doTest(xml, delimiter, ".", "#", "", expected, error, onRecordError, ignoreAttrs, ignoreNS, keepFields, newFieldOverwrites);
  }

  private void doTest(
      String xml,
      String delimiter,
      String fieldDelim,
      String attrDelim,
      String outputField,
      List<Record> expected,
      List<Record> error,
      OnRecordError onRecordError,
      boolean ignoreAttrs,
      boolean ignoreNS,
      boolean keepFields,
      boolean newFieldOverwrites
  ) throws Exception {
    processor = new XMLFlatteningProcessor(ORIGINAL, keepFields, newFieldOverwrites, outputField, delimiter, fieldDelim, attrDelim, ignoreAttrs, ignoreNS);
    ProcessorRunner runner = new ProcessorRunner.Builder(dProcessorClass, processor)
        .addOutputLane("xml").setOnRecordError(onRecordError).build();
    runner.runInit();
    Record r = createRecord(xml);
    if ((keepFields && !newFieldOverwrites) && !(keepFields && !outputField.isEmpty()) ) {
      r.set("/contact.name", Field.create("streamsets"));
    }
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r));
    for (int i = 0; i < expected.size(); i++) {
      Record currentExpected = expected.get(i);
      if (keepFields) {
        currentExpected.set(ORIGINAL, Field.create(xml));
        if (newFieldOverwrites && i == 0) {
          currentExpected.set("/contact.name", Field.create("NAME0"));
        }
      }
      Record result = output.getRecords().get("xml").get(i);
      // Comparing the root field is good enough
      Assert.assertEquals(currentExpected.get().getValueAsMap(), result.get().getValueAsMap());
    }
  }
}
