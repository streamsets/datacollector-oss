/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

  private String getInvalidXML() {
    return "<contact><name type=\"maiden\" xmlns=\"http://blah.com/blah.xml\">NAME</name>" +
        "<phone>(111)111-1111</phone><phone>(222)222-2222</phone></contact>" +
        "<contact><name type=\"maiden\" xmlns=\"http://blah.com/blah.xml\">NAME</name>" +
        "<phone>(111)111-1111</phone><phone>(222)222-2222</phone></contact>";
  }

  private LinkedHashMap<String, Field> createExpectedRecord(String prefix, String id, String offset, String delimStr,
                                                  String attrStr, boolean addAttrs, boolean addNS) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    String baseName = prefix + "contact" + offset + delimStr;

    if (addAttrs) {
      fields.put("contact" + attrStr +"type", Field.create("person"));
      fields.put(baseName + "name" + attrStr + "type", Field.create("maiden"));
    }

    if (addNS) {
      fields.put(baseName + "name#xmlns", Field.create("http://blah.com/blah.xml"));
    }
    fields.put(baseName + "name", Field.create("NAME" + id));
    fields.put(baseName + "phone(0)", Field.create("(111)111-1111" + id));
    fields.put(baseName + "phone(1)", Field.create("(222)222-2222" + id));
    return fields;
  }

  @Test
  public void testSingleRecordNoAttrsNoNS() throws Exception {
    String xml = getXML("");
    Record expected = RecordCreator.create();
    expected.set(Field.create(createExpectedRecord("", "", "", ".", "#", false, false)));
    doTest(xml, "contact", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, false, false);
  }

  @Test
  public void testSingleRecordAttrsNoNS() throws Exception {
    String xml = getXML("");
    Record expected = RecordCreator.create();
    expected.set(Field.create(createExpectedRecord("", "", "", ".", "#", true, false)));
    doTest(xml, "contact", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD, false, true, false, false);
  }

  @Test
  public void testSingleRecordNoAttrsNS() throws Exception {
    String xml = getXML("");
    Record expected = RecordCreator.create();
    expected.set(Field.create(createExpectedRecord("", "", "", ".", "#", false, true)));
    doTest(xml, "contact", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD, true, false, false, false);
  }

  @Test
  public void testSingleRecordAttrsNS() throws Exception {
    String xml = getXML("");
    Record expected = RecordCreator.create();
    expected.set(Field.create(createExpectedRecord("", "", "", "_", ".", true, true)));
    doTest(xml, "contact", "_", ".", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD,
        false, false, false, false);
  }

  @Test
  public void testMultipleRecords() throws Exception {
    String xml = "<contacts>" + getXML("0") + getXML("1") + "</contacts>";
    Record expected1 = RecordCreator.create();
    expected1.set(Field.create(createExpectedRecord("", "0", "", ".", "#",false, false)));
    Record expected2 = RecordCreator.create();
    expected2.set(Field.create(createExpectedRecord("", "1", "", ".", "#",false, false)));
    List<Record> expected = ImmutableList.of(expected1, expected2);
    doTest(xml, "contact", expected, Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, false, false);
  }

  @Test
  public void testMultipleRecordsNoDelimiter() throws Exception {
    String xml = "<contacts>" + getXML("0") + getXML("1") + "</contacts>";
    Record expected = RecordCreator.create();
    Map<String, Field> allFields = createExpectedRecord("contacts.", "0", "(0)", ".", "#", false, false);
    allFields.putAll(createExpectedRecord("contacts.", "1", "(1)", ".", "#", false, false));
    expected.set(Field.create(allFields));
    doTest(xml, "", ImmutableList.of(expected), Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, false, false);
  }

  @Test
  public void testInvalidRecordDiscard() throws Exception {
    doTestInvalidRecord(OnRecordError.DISCARD, false);
  }

  @Test
  public void testInvalidConfig() throws Exception {
    processor = new XMLFlatteningProcessor(ORIGINAL,false, false, "contact", "]", "[", true, true);
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
    processor = new XMLFlatteningProcessor(ORIGINAL, false, false, "<contact>", ".", "#", true, true);
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
    expected1.set(Field.createListMap(createExpectedRecord("", "0", "", ".", "#",false, false)));
    expected1.set("/contact.name", Field.create("streamsets"));
    Record expected2 = RecordCreator.create();
    expected2.set(Field.create(createExpectedRecord("", "1", "", ".", "#",false, false)));
    expected2.set("/contact.name", Field.create("streamsets"));
    List<Record> expected = ImmutableList.of(expected1, expected2);
    doTest(xml, "contact", expected, Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, true, false);
  }

  @Test
  public void testMultipleRecordsKeepOriginalFieldsOverWrite() throws Exception {
    String xml = "<contacts>" + getXML("0") + getXML("1") + "</contacts>";
    Record expected1 = RecordCreator.create();
    expected1.set(Field.create(createExpectedRecord("", "0", "", ".", "#",false, false)));
    expected1.set("/contact.name", Field.create("streamsets"));
    Record expected2 = RecordCreator.create();
    expected2.set(Field.create(createExpectedRecord("", "1", "", ".", "#",false, false)));
    List<Record> expected = ImmutableList.of(expected1, expected2);
    doTest(xml, "contact", expected, Collections.EMPTY_LIST, OnRecordError.DISCARD, true, true, true, true);
  }

  @Test
  public void testNonMap() throws Exception {
    Record expected1 = RecordCreator.create();
    expected1.set(Field.create(new ArrayList<Field>()));
    processor = new XMLFlatteningProcessor(ORIGINAL, true, false, "", "", "", false, false);
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

  private void doTest(String xml, String delimiter, List<Record> expected, List<Record> error, OnRecordError onRecordError,
      boolean ignoreAttrs, boolean ignoreNS, boolean keepFields, boolean newFieldOverwrites) throws Exception {
    doTest(xml, delimiter, ".", "#", expected, error, onRecordError, ignoreAttrs, ignoreNS, keepFields, newFieldOverwrites);
  }

  private void doTest(
      String xml,
      String delimiter,
      String fieldDelim,
      String attrDelim,
      List<Record> expected,
      List<Record> error,
      OnRecordError onRecordError,
      boolean ignoreAttrs,
      boolean ignoreNS,
      boolean keepFields,
      boolean newFieldOverwrites
  ) throws Exception {
    processor = new XMLFlatteningProcessor(ORIGINAL, keepFields, newFieldOverwrites, delimiter, fieldDelim, attrDelim, ignoreAttrs, ignoreNS);
    ProcessorRunner runner = new ProcessorRunner.Builder(dProcessorClass, processor)
        .addOutputLane("xml").setOnRecordError(onRecordError).build();
    runner.runInit();
    Record r = createRecord(xml);
    if (keepFields && !newFieldOverwrites) {
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
