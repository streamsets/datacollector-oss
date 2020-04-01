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
package com.streamsets.pipeline.stage.processor.javascript;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ScriptingProcessorTestUtil;
import com.streamsets.pipeline.stage.util.scripting.config.ScriptRecordType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestJavaScriptProcessor {

  @Test
  public void testJavascriptAllTypes() throws Exception {
    Processor processor = new JavaScriptProcessor(ProcessingMode.RECORD,
        "var record = sdc.records[0];\n" +
        "record.value['newField'] = {\n" +
            "a: {\n" +
              "b: record.value['beginner'], \n" +
              "c: record.value['skilled']\n" +
            "}, \n" +
            "d: ['str1', 'str2'], \n" +
            "e: record.value['expert'] \n" +
          "};\n" +
        "sdc.output.write(record);");
    ProcessorRunner runner = new ProcessorRunner.Builder(JavaScriptDProcessor.class, processor)
      .addOutputLane("lane")
      .build();
    runner.runInit();
    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("false"));
      map.put("intermediate", Field.create("yes"));
      map.put("advanced", Field.create("no"));
      map.put("expert", Field.create(true));
      map.put("skilled", Field.create(122345566));
      map.put("null", Field.create(Field.Type.STRING, null));

      List<Field> list = ImmutableList.of(Field.create("listString1"), Field.create("listString2"));
      map.put("list", Field.create(list));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Field field = output.getRecords().get("lane").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 8);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals("false", result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals("yes", result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals("no", result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(true, result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(122345566, result.get("skilled").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
      Assert.assertTrue(result.containsKey("list"));
      List<Field> listField = result.get("list").getValueAsList();
      Assert.assertTrue(listField.size() == 2);
      Assert.assertEquals("listString1", listField.get(0).getValueAsString());
      Assert.assertEquals("listString2", listField.get(1).getValueAsString());

      //Field added by Javascript Evaluator
      Assert.assertTrue(result.containsKey("newField"));
      Map<String, Field> newField = result.get("newField").getValueAsMap();
      Assert.assertTrue(newField.containsKey("a"));
      Assert.assertTrue(newField.get("a").getValueAsMap().containsKey("b"));
      Assert.assertEquals("false", newField.get("a").getValueAsMap().get("b").getValueAsString());
      Assert.assertTrue(newField.get("a").getValueAsMap().containsKey("c"));
      Assert.assertEquals(122345566, newField.get("a").getValueAsMap().get("c").getValue());
      Assert.assertTrue(newField.containsKey("d"));
      Assert.assertEquals("str1", newField.get("d").getValueAsList().get(0).getValueAsString());
      Assert.assertEquals("str2", newField.get("d").getValueAsList().get(1).getValueAsString());
      Assert.assertTrue(newField.containsKey("e"));
      Assert.assertEquals(true, newField.get("e").getValueAsBoolean());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testJavascriptMapArray() throws Exception {
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        "sdc.output.write(sdc.records[0]);\n" +
        "sdc.records[0].value = {};\n" +
        "sdc.records[0].value = 'Hello';\n" +
        "sdc.output.write(sdc.records[0]);\n" +
        "sdc.records[0].value = { 'foo' : 'FOO' };\n" +
        "sdc.output.write(sdc.records[0]);\n" +
        "sdc.records[0].value = [ 5 ];\n" +
        "sdc.output.write(sdc.records[0]);\n" +
        ""
    );
    ScriptingProcessorTestUtil.verifyMapAndArray(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testJavaScriptFileRefField() throws Exception {
    String script = "var record = sdc.records[0];\n" +
        "var fileRef = record.value['fileRef'];\n" +
        "var is = fileRef.getInputStream();\n" +
        "var b = [];\n" +
        "var read = 0;\n" +
        "do {\n" +
        "    read = is.read();\n" +
        "    if (read == -1) \n" +
        "        break;\n" +
        "    b.push(read)\n" +
        "} while (true);\n" +
        "is.close();\n" +
        "record.value['byte_array'] = b;\n" +
        "sdc.output.write(records[0]);";

    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        script
    );
    ScriptingProcessorTestUtil.verifyFileRef(JavaScriptDProcessor.class, processor);
  }


  private void testMode(ProcessingMode mode) throws Exception {
    Processor processor = new JavaScriptProcessor(
        mode,
        "for (var i = 0; i < sdc.records.length; i++){\n" +
        "  sdc.output.write(records[i]);\n" +
        "}"
    );
    ScriptingProcessorTestUtil.verifyMode(JavaScriptDProcessor.class, processor);
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
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        "for (var i = 0; i < sdc.records.length; i++){\n" +
        "  var record = sdc.records[i];" +
        "  if (record.value == 'Hello') {\n" +
        "    throw 'Exception';\n" +
        "  }" +
        "  sdc.output.write(record);" +
        "}"
    );
    ScriptingProcessorTestUtil.verifyRecordModeOnErrorHandling(JavaScriptDProcessor.class, processor, onRecordError);
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
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.BATCH,
        "for (var i = 0; i < sdc.records.length; i++){\n" +
        "  var record = sdc.records[i];" +
        "  if (record.value == 'Hello') {\n" +
        "    throw 'Exception';\n" +
        "  }" +
        "  sdc.output.write(record);" +
        "}"
    );
    ScriptingProcessorTestUtil.verifyBatchModeOnErrorHandling(JavaScriptDProcessor.class, processor, onRecordError);
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
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.BATCH,
        "for (var i = 0; i < sdc.records.length; i++){\n" +
        "  sdc.output.write(records[i]);\n" +
        "}"
    );
    ScriptingProcessorTestUtil.verifyPrimitiveTypesPassthrough(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testPrimitiveTypesFromScripting() throws Exception {
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.BATCH,
        "for (var i = 0; i < records.length; i++){\n" +
        "  records[i].value = [ 1, 0.5, true, 'hello' ];\n" +
        "  output.write(records[i]);\n" +
        "  records[i].value = null;\n" +
        "  output.write(records[i]);\n" +
        "}"
    );
    ScriptingProcessorTestUtil.verifyPrimitiveTypesFromScriptingJavaScript(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testStateObject() throws Exception {
    Processor processor = new JavaScriptProcessor(ProcessingMode.RECORD,
        "if (!sdc.state['total_count']) {\n" +
        "  sdc.state['total_count'] = 0;\n" +
        "}\n" +
        "sdc.state['total_count'] = sdc.state['total_count'] + sdc.records.length;\n" +
        "for (var i = 0; i < sdc.records.length; i++) {\n" +
        "  sdc.records[i].value['count'] = sdc.state['total_count'];\n" +
        "  sdc.output.write(records[i]);\n" +
        "}");
    ScriptingProcessorTestUtil.verifyStateObjectJavaScript(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testListMap() throws Exception {
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        "sdc.output.write(sdc.records[0]);\n" +
        "sdc.records[0].value['Hello'] = 2\n" +
        "sdc.output.write(records[0])\n" +
        ""
    );
    ScriptingProcessorTestUtil.verifyListMap(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testMapCreation() throws Exception {
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        "newMap = sdc.createMap(true)\n" +
            "newMap['Key'] = 'streamsets'\n" +
            "sdc.records[0].value['Test'] = newMap\n" +
            "sdc.output.write(sdc.records[0])\n" +
            "newRecord = sdc.createRecord('id')\n" +
            "rootMap = sdc.createMap(true)\n" +
            "rootMap['Hello'] = 2\n" +
            "newRecord.value = rootMap\n" +
            "newMap2 = sdc.createMap(false)\n" +
            "newMap2['Key'] = 'dpm'\n" +
            "newRecord.value['Test'] = newMap2\n" +
            "sdc.output.write(newRecord)"
    );
    ScriptingProcessorTestUtil.verifyMapListMapCreation(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testEventCreation() throws Exception {
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        "event = sdc.createEvent(\"not important\", 1)\n" +
            "event.value = {\"a\": 1, \"b\" :2, \"c\": 3}\n" +
            "sdc.toEvent(event)"
    );
    ScriptingProcessorTestUtil.verifyEventCreation(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testTypedNullPassThrough() throws Exception {
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.BATCH,
        "for (var i = 0; i < sdc.records.length; i++){\n" +
            "  sdc.records[i].value['new_field'] = 'testtest';\n" +
            "  sdc.output.write(sdc.records[i]);\n" +
            "}"
    );
    ScriptingProcessorTestUtil.verifyPreserveTypeForNullValue(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testAssignNullToTypedField() throws Exception {
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.BATCH,
        "for (var i = 0; i < sdc.records.length; i++){\n" +
            // record.value will be an array
            "  for(var index=0; index < sdc.records[i].value.length; index++){\n" +
            "    sdc.records[i].value[index] = null;\n" +
            "  }\n" +
            "  sdc.output.write(sdc.records[i]);\n" +
            "}"
    );
    ScriptingProcessorTestUtil.verifyPreserveTypeForNullValue(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testNestedMapWithNull() throws Exception {
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.BATCH,
        "for (var i = 0; i < sdc.records.length; i++){\n" +
            "  for(var key in sdc.records[i].value.row1) {\n" +
            "      sdc.records[i].value.row1[key] = null;\n" +
            "  }\n" +
            "  sdc.records[i].value.row2 = null;\n" +
            "  sdc.output.write(records[i]);\n" +
            "}"
    );
    ScriptingProcessorTestUtil.verifyNestedMap(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testChangeFieldTypeFromScripting() throws Exception {
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.BATCH,
        // record.value will be a map
        "for (var i = 0; i < sdc.records.length; i++){\n" +
            "  sdc.records[i].value.long_bool = true;\n" +
            "  sdc.records[i].value.str_date = new Date();\n" +
            "  sdc.output.write(sdc.records[i]);" +
            "}"
    );
    ScriptingProcessorTestUtil.verifyChangedTypeFromScripting(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testListMapOrder() throws Exception {
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        "sdc.records[0].value['A0'] = 0\n" +
        "sdc.records[0].value['A1'] = 1\n" +
        "sdc.records[0].value['A2'] = 2\n" +
        "sdc.records[0].value['A3'] = 3\n" +
        "sdc.records[0].value['A4'] = 4\n" +
        "sdc.records[0].value['A5'] = 5\n" +
        "sdc.records[0].value['A6'] = 6\n" +
        "sdc.records[0].value['A7'] = 7\n" +
        "sdc.records[0].value['A8'] = 8\n" +
        "sdc.records[0].value['A9'] = 9\n" +
        "sdc.records[0].value['A10'] = 10\n" +
        "sdc.records[0].value['A11'] = 11\n" +
        "sdc.records[0].value['A12'] = 12\n" +
        "sdc.records[0].value['A13'] = 13\n" +
        "sdc.records[0].value['A14'] = 14\n" +
        "sdc.records[0].value['A15'] = 15\n" +
        "sdc.records[0].value['A16'] = 16\n" +
        "sdc.records[0].value['A17'] = 17\n" +
        "sdc.records[0].value['A18'] = 18\n" +
        "sdc.records[0].value['A19'] = 19\n" +
        "sdc.output.write(sdc.records[0])\n" +
            ""
    );
    ScriptingProcessorTestUtil.verifyListMapOrder(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testNewFieldWithTypedNull() throws Exception {
    // initial data in record is empty
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    record.set(Field.create(map));

    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        "for (var i = 0; i < sdc.records.length; i++){\n" +
            "  sdc.records[i].value.null_int = sdc.NULL_INTEGER;\n" +
            "  sdc.records[i].value.null_long = sdc.NULL_LONG;\n" +
            "  sdc.records[i].value.null_float = sdc.NULL_FLOAT;\n" +
            "  sdc.records[i].value.null_double = sdc.NULL_DOUBLE;\n" +
            "  sdc.records[i].value.null_date = sdc.NULL_DATE;\n" +
            "  sdc.records[i].value.null_datetime = sdc.NULL_DATETIME;\n" +
            "  sdc.records[i].value.null_boolean = sdc.NULL_BOOLEAN;\n" +
            "  sdc.records[i].value.null_decimal = sdc.NULL_DECIMAL;\n" +
            "  sdc.records[i].value.null_byteArray = sdc.NULL_BYTE_ARRAY;\n" +
            "  sdc.records[i].value.null_string = sdc.NULL_STRING;\n" +
            "  sdc.output.write(sdc.records[i]);\n" +
        "}"
    );

    ScriptingProcessorTestUtil.verifyTypedFieldWithNullValue(JavaScriptDProcessor.class, processor, record);
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
    // add list field
    List<Field> list1 = new LinkedList<>();
    list1.add(Field.create("dummy field list"));
    map.put("null_list", Field.create(list1));
    // add map field
    Map<String, Field> map1 = new HashMap<>();
    map1.put("dummy", Field.create("dummy field map"));
    map.put("null_map", Field.create(map1));


    record.set(Field.create(map));

    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        "for (var i = 0; i < sdc.records.length; i++){\n" +
            "  sdc.records[i].value.null_int = sdc.NULL_INTEGER;\n" +
            "  sdc.records[i].value.null_date = sdc.NULL_DATE;\n" +
            "  sdc.records[i].value.null_decimal = sdc.NULL_DECIMAL;\n" +
            "  sdc.records[i].value.null_string = sdc.NULL_STRING;\n" +
            "  sdc.records[i].value.null_list = sdc.NULL_LIST;\n" +
            "  sdc.records[i].value.null_map = sdc.NULL_MAP;\n" +
            "  sdc.output.write(sdc.records[i]);\n" +
            "}"
    );
    ScriptingProcessorTestUtil.verifyTypedFieldWithNullValue(JavaScriptDProcessor.class, processor,record);
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

    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        "for (var i = 0; i < sdc.records.length; i++){\n" +
            "  if(sdc.getFieldNull(sdc.records[i], '/null_int') == sdc.NULL_INTEGER)\n" +
            "    sdc.records[i].value.null_int = 123; \n" +
            "  if(sdc.getFieldNull(sdc.records[i], '/null_string') == sdc.NULL_STRING)\n" +
            "    sdc.records[i].value.null_string = 'test'; \n" +
            "  if(sdc.getFieldNull(sdc.records[i], '/null_boolean') == sdc.NULL_BOOLEAN)\n" +
            "    sdc.records[i].value.null_boolean = true; \n" +
            "  if(sdc.getFieldNull(sdc.records[i], '/null_list') == sdc.NULL_LIST)\n" +
            "    sdc.records[i].value.null_list = ['elem1', 'elem2']; \n" +
            "  if(sdc.getFieldNull(sdc.records[i], '/null_map') == sdc.NULL_MAP)\n" +
            "    sdc.records[i].value.null_map = {x: 'X', y: 'Y'}; \n" +
            "  if(sdc.getFieldNull(sdc.records[i], '/null_datetime') == sdc.NULL_DATETIME)\n" + // this should be false
            "    sdc.records[i].value.null_datetime = sdc.NULL_DATETIME \n" +
            "  sdc.output.write(records[i]);\n" +
            "}"
    );

    ScriptingProcessorTestUtil.verifyNullField(JavaScriptDProcessor.class, processor,record);
  }

  @Test
  public void testCreateRecordWithNewRecordId() throws Exception {
    String recordId = "recordId";
    String script = "for (var i = 0; i < sdc.records.length; i++) {\n" +
        "  try {\n" +
        "    var newRecord = sdc.createRecord('" + recordId + "');\n" +
        "    newRecord.value = {'record_value' :'record_value'};\n" +
        "    sdc.output.write(records[i]);\n" +
        "    sdc.output.write(newRecord);\n" +
        "  } catch (e) {\n" +
        "    // Send record to error\n" +
        "    sdc.error.write(records[i], e);\n" +
        "  }\n" +
        "}";

    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        script
    );

    ScriptingProcessorTestUtil.verifyCreateRecord(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testRecordHeaderAttributes() throws Exception {
    String headerKey = "key1";
    String value = "value1";
    String script = "for (var i = 0; i < sdc.records.length; i++) {\n" +
        "  sdc.records[i].attributes['" + headerKey + "'] = '" + value + "'\n" +
        "  sdc.records[i].attributes.remove('remove')\n" +
        "  sdc.output.write(sdc.records[i])\n" +
        "}";

    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        script
    );

    Record record = RecordCreator.create();
    ScriptingProcessorTestUtil.verifyRecordHeaderAttribute(JavaScriptDProcessor.class, processor, record);
  }

  @Test
  public void testAccessSdcRecord() throws Exception {
    String script = "for (var i = 0; i < sdc.records.length; i++) {\n" +
        "  sdc.records[i].attributes['attr'] = sdc.records[i].sdcRecord.get('/value').getAttribute('attr')\n" +
        "  sdc.output.write(sdc.records[i])\n" +
        "}";

    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        script
    );

    ScriptingProcessorTestUtil.verifyAccessToSdcRecord(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testInitDestroy() throws Exception {
    String initScript = "sdc.state['initValue'] = 'init';";
    String script = "for (var i = 0; i < sdc.records.length; i++) {\n" +
        "  sdc.records[i].value['initValue'] = sdc.state['initValue'];\n" +
        "  sdc.output.write(records[i])\n" +
        "}";
    String destroyScript = "event = sdc.createEvent(\"event\", 1)\n" +
      "sdc.toEvent(event)";

    Processor processor = new JavaScriptProcessor(
        ProcessingMode.BATCH,
        script,
        initScript,
        destroyScript,
        ScriptRecordType.NATIVE_OBJECTS,
        new HashMap<>()
    );
    ScriptingProcessorTestUtil.verifyInitDestroy(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testConstants() throws Exception {
    String script = "for (var i = 0; i < sdc.records.length; i++) {\n" +
        "  sdc.records[i].value['company'] = sdc.pipelineParameters()['company'];\n" +
        "  sdc.output.write(sdc.records[i]);\n" +
        "}";
    Processor processor = new JavaScriptProcessor(ProcessingMode.BATCH, script);
    ScriptingProcessorTestUtil.verifyConstants(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testIsPreview() throws Exception {
    String script = "for (var i = 0; i < sdc.records.length; i++) {\n" +
        "  sdc.records[i].value['isPreview'] = sdc.isPreview();\n" +
        "  sdc.output.write(sdc.records[i]);\n" +
        "}";
    Processor processor = new JavaScriptProcessor(ProcessingMode.BATCH, script);
    ScriptingProcessorTestUtil.verifyIsPreview(JavaScriptDProcessor.class, processor);
  }

  private static final String WRITE_ERROR_SCRIPT = "for(var i = 0; i < sdc.records.length; i++) { sdc.error.write(sdc.records[i], 'oops'); }";

  @Test
  public void testErrorRecordStopPipeline() throws Exception {
    Processor processor = new JavaScriptProcessor(
      ProcessingMode.RECORD,
      WRITE_ERROR_SCRIPT
    );
    ScriptingProcessorTestUtil.verifyErrorRecordStopPipeline(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testErrorRecordDiscard() throws Exception {
    Processor processor = new JavaScriptProcessor(
      ProcessingMode.RECORD,
      WRITE_ERROR_SCRIPT
    );
    ScriptingProcessorTestUtil.verifyErrorRecordDiscard(JavaScriptDProcessor.class, processor);
  }


  @Test
  public void testErrorRecordErrorSink() throws Exception {
    Processor processor = new JavaScriptProcessor(
      ProcessingMode.RECORD,
      WRITE_ERROR_SCRIPT
    );
    ScriptingProcessorTestUtil.verifyErrorRecordErrorSink(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testSdcRecord() throws Exception {
    String script = "var Field = Java.type('com.streamsets.pipeline.api.Field');\n" +
      "for (var i = 0; i < sdc.records.length; i++) {\n" +
      "  sdc.records[i].sdcRecord.set('/new', Field.create(Field.Type.STRING, 'new-value'));\n" +
      "  sdc.records[i].sdcRecord.get('/old').setAttribute('attr', 'attr-value');\n" +
      "  sdc.output.write(sdc.records[i])\n" +
      "}";

    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        script,
        "",
        "",
        ScriptRecordType.SDC_RECORDS,
        new HashMap<>()
    );
    ScriptingProcessorTestUtil.verifySdcRecord(JavaScriptDProcessor.class, processor);
  }

  @Test
  public void testUserParams() throws Exception {
    String script = "for(var i = 0; i < sdc.records.length; i++) {\n" +
        "  sdc.records[i].value['user-param-key'] = sdc.userParams['user-param-key'];\n" +
        "  output.write(sdc.records[i]);\n" +
        "}";
    Map<String, String> userParams = new HashMap<>();
    userParams.put("user-param-key", "user-param-value");
    JavaScriptProcessor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        script,
        "",
        "",
        ScriptRecordType.NATIVE_OBJECTS,
        userParams
     );
    ScriptingProcessorTestUtil.verifyUserParams(JavaScriptDProcessor.class, processor);
  }

  @Test
  @Deprecated
  public void testDeprecatedBindings() throws Exception {
    List<String> allNames = new ArrayList<>();
    allNames.addAll(ScriptingProcessorTestUtil.renames.keySet());
    allNames.addAll(ScriptingProcessorTestUtil.renames.values());
    String script = ScriptingProcessorTestUtil.writeBindingTestScript(
        "var isListMap = false;\n" +
            "for(var i = 0; i < sdc.records.length; i++) {\n",
        "  sdc.records[i].value['%s'] = %s;\n",
        "  sdc.output.write(records[i]);\n" +
        "}",
        allNames
    );
    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        script,
        "",
        "",
        ScriptRecordType.NATIVE_OBJECTS,
        new HashMap<>()
    );
    ScriptingProcessorTestUtil.verifyDeprecatedBindings(
        JavaScriptDProcessor.class,
        processor
    );
  }

  @Test
  public void testUserCodeInjectionFlag() {
    Assert.assertArrayEquals(
        "This stage should _only_ have the USER_CODE_INJECTION flag set",
        new StageBehaviorFlags[]{StageBehaviorFlags.USER_CODE_INJECTION},
        JavaScriptDProcessor.class.getAnnotation(StageDef.class).flags()
    );
  }

  @Test
  public void testNativeNullRootField() {
    String script = "records[0].value = null;\n" +
        "sdc.output.write(records[0]);\n";

    Processor processor = new JavaScriptProcessor(
        ProcessingMode.RECORD,
        script,
        "",
        "",
        ScriptRecordType.NATIVE_OBJECTS,
        Collections.emptyMap()
    );
    ScriptingProcessorTestUtil.verifyNativeNullRootValue(JavaScriptDProcessor.class, processor);
  }
}
