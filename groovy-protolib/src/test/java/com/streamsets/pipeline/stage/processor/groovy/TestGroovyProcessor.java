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
package com.streamsets.pipeline.stage.processor.groovy;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ScriptingProcessorTestUtil;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestGroovyProcessor {

  @Test
  public void testGroovyAndMapArray() throws Exception {
    final String script = Resources.toString(Resources.getResource("MapAndArrayScript.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.RECORD, script);

    ScriptingProcessorTestUtil.verifyMapAndArray(GroovyDProcessor.class, processor);
  }

  private void testMode(ProcessingMode mode) throws Exception {
    final String script = Resources.toString(Resources.getResource("ModeScript.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(mode, script);

    ScriptingProcessorTestUtil.verifyMode(GroovyDProcessor.class, processor);
  }

  @Test
  public void testGroovyFileRefField() throws Exception {
    String script = "for (record in records) {\n" +
        "  try {\n" +
        "\tfileRef = record.value['fileRef'];\n" +
        "\tis = fileRef.getInputStream();\n" +
        "\tb = [];\n" +
        "\twhile ((read = is.read()) != -1) {\n" +
        "    \tb.add(read)\n" +
        "\t}\n" +
        "\t is.close();\n"+
        "\trecord.value['byte_array'] = b;\n" +
        "    output.write(record)\n" +
        "  } catch (e) {\n" +
        "    // Write a record to the error pipeline\n" +
        "    log.error(e.toString(), e)\n" +
        "    error.write(record, e.toString())\n" +
        "  }\n" +
        "}";

    Processor processor = new GroovyProcessor(
        ProcessingMode.RECORD,
        script
    );
   ScriptingProcessorTestUtil.verifyFileRef(GroovyDProcessor.class, processor);
  }

  @Test
  public void testRecordMode() throws Exception {
    testMode(ProcessingMode.RECORD);
  }

  @Test
  public void testBatchMode() throws Exception {
    testMode(ProcessingMode.BATCH);
  }

  private void testRecordModeOnErrorHandling(OnRecordError onRecordError) throws Exception {
    final String script = Resources.toString(Resources.getResource("OnErrorHandlingScript.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.RECORD, script);

    ScriptingProcessorTestUtil.verifyRecordModeOnErrorHandling(GroovyDProcessor.class, processor, onRecordError);
  }

  @Test
  public void testRecordOnErrorDiscard() throws Exception {
    testRecordModeOnErrorHandling(OnRecordError.DISCARD);
  }

  @Test
  public void testRecordOnErrorToError() throws Exception {
    testRecordModeOnErrorHandling(OnRecordError.TO_ERROR);
  }

  @Test(expected = StageException.class)
  public void testRecordOnErrorStopPipeline() throws Exception {
    testRecordModeOnErrorHandling(OnRecordError.STOP_PIPELINE);
  }

  private void testBatchModeOnErrorHandling(OnRecordError onRecordError) throws Exception {
    final String script = Resources.toString(Resources.getResource("OnErrorHandlingScript.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.BATCH, script);

    ScriptingProcessorTestUtil.verifyBatchModeOnErrorHandling(GroovyDProcessor.class, processor, onRecordError);
  }

  @Test(expected = StageException.class)
  public void testBatchOnErrorDiscard() throws Exception {
    testBatchModeOnErrorHandling(OnRecordError.DISCARD);
  }

  @Test(expected = StageException.class)
  public void testBatchOnErrorToError() throws Exception {
    testBatchModeOnErrorHandling(OnRecordError.TO_ERROR);
  }

  @Test(expected = StageException.class)
  public void testBatchOnErrorStopPipeline() throws Exception {
    testBatchModeOnErrorHandling(OnRecordError.STOP_PIPELINE);
  }

  @Test
  public void testPrimitiveTypesPassthrough() throws Exception {
    final String script = Resources.toString(Resources.getResource("PrimitiveTypesPassthroughScript.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.RECORD, script);

    ScriptingProcessorTestUtil.verifyPrimitiveTypesPassthrough(GroovyDProcessor.class, processor);
  }

  @Test
  public void testPrimitiveTypesFromScripting() throws Exception {
    final String script = Resources.toString(
        Resources.getResource("PrimitiveTypesFromScripting.groovy"),
        Charsets.UTF_8
    );
    Processor processor = new GroovyProcessor(ProcessingMode.RECORD, script);

    ScriptingProcessorTestUtil.verifyPrimitiveTypesFromScripting(GroovyDProcessor.class, processor);
  }

  @Test
  public void testStateObject() throws Exception {
    final String script = Resources.toString(Resources.getResource("StateObjectScript.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.RECORD, script);

    ScriptingProcessorTestUtil.verifyStateObject(GroovyDProcessor.class, processor);
  }

  @Test
  public void testListMap() throws Exception {
    final String script = Resources.toString(Resources.getResource("ListMapScript.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.RECORD, script);

    ScriptingProcessorTestUtil.verifyListMap(GroovyDProcessor.class, processor);
  }

  @Test
  public void testMapCreation() throws Exception {
    Processor processor = new GroovyProcessor(
        ProcessingMode.RECORD,
        "newMap = sdcFunctions.createMap(true)\n" +
            "newMap['Key'] = 'streamsets'\n" +
            "records[0].value['Test'] = newMap\n" +
            "output.write(records[0])\n" +
            "newRecord = sdcFunctions.createRecord('id')\n" +
            "rootMap = sdcFunctions.createMap(true)\n" +
            "rootMap['Hello'] = 2\n" +
            "newRecord.value = rootMap\n" +
            "newMap2 = sdcFunctions.createMap(false)\n" +
            "newMap2['Key'] = 'dpm'\n" +
            "newRecord.value['Test'] = newMap2\n" +
            "output.write(newRecord)"
    );
    ScriptingProcessorTestUtil.verifyMapListMapCreation(GroovyDProcessor.class, processor);
  }

  @Test
  public void testEventCreation() throws Exception {
    Processor processor = new GroovyProcessor(
        ProcessingMode.RECORD,
        "event = sdcFunctions.createEvent(\"not important\", 1)\n" +
            "event.value = [\"a\": 1, \"b\" :2, \"c\": 3]\n" +
            "sdcFunctions.toEvent(event)"
    );
    ScriptingProcessorTestUtil.verifyEventCreation(GroovyDProcessor.class, processor);
  }

  @Test
  public void testListMapOrder() throws Exception {
    final String script = Resources.toString(Resources.getResource("ListMapOrderScript.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.RECORD, script);

    ScriptingProcessorTestUtil.verifyListMapOrder(GroovyDProcessor.class, processor);
  }

  @Test
  public void testTypedNullPassThrough() throws Exception {
    final String script = Resources.toString(Resources.getResource("PrimitiveTypesPassthroughScript.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.BATCH, script);
    ScriptingProcessorTestUtil.verifyPreserveTypeForNullValue(GroovyDProcessor.class, processor);
  }

  @Test
  public void testAssignNullToTypedField() throws Exception {
    final String script = Resources.toString(Resources.getResource("AssignNullToTypedField.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.BATCH, script);
    ScriptingProcessorTestUtil.verifyPreserveTypeForNullValue(GroovyDProcessor.class, processor);
  }

  @Test
  public void testNestedMapWithNull() throws Exception {
    final String script = Resources.toString(Resources.getResource("NestedMapWithNull.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.BATCH, script);
    ScriptingProcessorTestUtil.verifyNestedMap(GroovyDProcessor.class, processor);
  }

  @Test
  public void testChangeFieldTypeFromScripting() throws Exception {
    final String script = Resources.toString(Resources.getResource("ChangeFieldTypeScript.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.BATCH,script);
    ScriptingProcessorTestUtil.verifyChangedTypeFromScripting(GroovyDProcessor.class, processor);
  }

  @Test
  public void testNewFieldWithTypedNull() throws Exception {
    // initial data in record is empty
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    record.set(Field.create(map));

    final String script = Resources.toString(Resources.getResource("AssignTypedNullField.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.BATCH,script);
    ScriptingProcessorTestUtil.verifyTypedFieldWithNullValue(GroovyDProcessor.class, processor, record);
  }

  @Test
  public void testChangeFieldToTypedNull() throws Exception {
    // initial data in record
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("null_int", Field.create("this is string field"));
    map.put("null_string", Field.create(123L));
    map.put("null_date", Field.create(true));
    map.put("null_decimal", Field.createDate(null));
    map.put("null_short", Field.create((short)1000));
    map.put("null_char", Field.create('c'));
    // add a list field
    List<Field> list1 = new LinkedList<>();
    list1.add(Field.create("dummy field list"));
    map.put("null_list", Field.create(list1));
    record.set(Field.create(map));

    final String script = Resources.toString(Resources.getResource("AssignTypedNullField.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.BATCH, script);
    ScriptingProcessorTestUtil.verifyTypedFieldWithNullValue(GroovyDProcessor.class, processor, record);
  }

  @Test
  public void testGetFieldNull() throws Exception {
    // initial data in record
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("null_int", Field.create(Field.Type.INTEGER, null));
    map.put("null_string", Field.create(Field.Type.STRING, null));
    map.put("null_boolean", Field.create(Field.Type.BOOLEAN,null));
    map.put("null_list", Field.create(Field.Type.LIST, null));
    map.put("null_map", Field.create(Field.Type.MAP, null));
    // original record has value in the field, so getFieldNull should return the value
    map.put("null_datetime", Field.createDatetime(new Date()));
    record.set(Field.create(map));

    final String script = Resources.toString(Resources.getResource("GetFieldNullScript.groovy"), Charsets.UTF_8);
    Processor processor = new GroovyProcessor(ProcessingMode.BATCH, script);
    ScriptingProcessorTestUtil.verifyNullField(GroovyDProcessor.class, processor,record);
  }

  @Test
  public void testCreateRecordWithNewRecordId() throws Exception {
    String recordId = "recordId";
    String script = "for (record in records) {\n" +
        "  try {\n" +
        "    newRecord = sdcFunctions.createRecord('" + recordId + "');\n" +
        "    newRecord.value = ['record_value':'record_value']\n" +
        "    output.write(record)\n" +
        "    output.write(newRecord)\n" +
        "  } catch (e) {\n" +
        "    // Write a record to the error pipeline\n" +
        "    log.error(e.toString(), e)\n" +
        "    error.write(record, e.toString())\n" +
        "  }\n" +
        "}";

    Processor processor = new GroovyProcessor(
        ProcessingMode.RECORD,
        script
    );
    ScriptingProcessorTestUtil.verifyCreateRecord(GroovyDProcessor.class, processor);
  }

  @Test
  public void testRecordHeaderAttributes() throws Exception {
    String headerKey = "key1";
    String value = "value1";

    String script = "for (record in records) {\n" +
        "  record.attributes['" + headerKey + "'] = '" + value + "'\n" +
        "  output.write(record)\n" +
        "}";

    Processor processor = new GroovyProcessor(
        ProcessingMode.RECORD,
        script
    );

    Record record = RecordCreator.create();
    ScriptingProcessorTestUtil.verifyRecordHeaderAttribute(GroovyDProcessor.class, processor, record);
  }

  @Test
  public void testInitDestroy() throws Exception {
    String initScript = "state['initValue'] = 'init'";
    String script = "for (record in records) {\n" +
        "  record.value['initValue'] = state['initValue']\n" +
        "  output.write(record)\n" +
        "}";
    String destroyScript = "event = sdcFunctions.createEvent(\"event\", 1)\n" +
      "sdcFunctions.toEvent(event)";

    Processor processor = new GroovyProcessor(
        ProcessingMode.BATCH,
        script,
        initScript,
        destroyScript
    );
    ScriptingProcessorTestUtil.verifyInitDestroy(GroovyDProcessor.class, processor);
  }

  @Test
  public void testConstants() throws Exception {
    String script = "for(record in records) {\n" +
        "  record.value['company'] = sdcFunctions.pipelineParameters()['company'];\n" +
        "  output.write(record);\n" +
        "}";
    Processor processor = new GroovyProcessor(ProcessingMode.BATCH, script);
    ScriptingProcessorTestUtil.verifyConstants(GroovyDProcessor.class, processor);
  }

  private static final String WRITE_ERROR_SCRIPT = "for (record in records) { error.write(record, 'oops'); }";

  @Test
  public void testErrorRecordStopPipeline() throws Exception {
    Processor processor = new GroovyProcessor(
      ProcessingMode.RECORD,
      WRITE_ERROR_SCRIPT
    );
    ScriptingProcessorTestUtil.verifyErrorRecordStopPipeline(GroovyDProcessor.class, processor);
  }

  @Test
  public void testErrorRecordDiscard() throws Exception {
    Processor processor = new GroovyProcessor(
      ProcessingMode.RECORD,
      WRITE_ERROR_SCRIPT
    );
    ScriptingProcessorTestUtil.verifyErrorRecordDiscard(GroovyDProcessor.class, processor);
  }


  @Test
  public void testErrorRecordErrorSink() throws Exception {
    Processor processor = new GroovyProcessor(
      ProcessingMode.RECORD,
      WRITE_ERROR_SCRIPT
    );
    ScriptingProcessorTestUtil.verifyErrorRecordErrorSink(GroovyDProcessor.class, processor);
  }
}
