/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.jython;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.SourceResponseSink;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SinkUtils;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.scripting.config.ScriptRecordType;
import com.streamsets.pipeline.stage.origin.scripting.config.ScriptSourceConfigBean;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestJythonSource {

  public String getScript(String scriptName, Class searchClass) {
    try {
      URL url = Resources.getResource(searchClass, scriptName);
      return Resources.toString(url, Charsets.UTF_8);
    } catch (IOException e) {
      System.out.println(e);
      return null;
    }
  }

  public String getScript(String scriptName) {
    return getScript(scriptName, TestJythonSource.class);
  }

  @Test
  public void testGenerateRecords() throws Exception {
    ScriptSourceConfigBean scriptConf = new ScriptSourceConfigBean();
    scriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    scriptConf.params = new HashMap<>();
    scriptConf.numThreads = 1;
    scriptConf.batchSize = 1000;
    JythonDSource source = new JythonDSource();
    source.scriptConf = scriptConf;
    PushSourceRunner runner = new PushSourceRunner.Builder(JythonDSource.class, source)
        .addConfiguration("script", getScript("GeneratorOriginScript.py", JythonSource.class))
        .addOutputLane("lane")
        .build();
    runner.runInit();
    Map<String, String> offsets = new HashMap<>();
    final List<Record> records = Collections.synchronizedList(new ArrayList<>());
    runner.runProduce(offsets, scriptConf.batchSize, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.addAll(output.getRecords().get("lane"));
          runner.setStop();
        }
      });
    runner.waitOnProduce();
    List<EventRecord> events = runner.getEventRecords();
    List<String> errors = runner.getErrors();
    runner.runDestroy();
  }

  @Test
  public void testMultiThreadGenerateRecords() {
    ScriptSourceConfigBean scriptConf = new ScriptSourceConfigBean();
    scriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    scriptConf.params = new HashMap<>();
    scriptConf.numThreads = 10;
    scriptConf.batchSize = 1000;
    JythonDSource source = new JythonDSource();
    source.scriptConf = scriptConf;
    PushSourceRunner runner = new PushSourceRunner.Builder(JythonDSource.class, source)
        .addConfiguration("script", getScript("MultiGeneratorOriginScript.py"))
        .addOutputLane("lane")
        .build();
    runner.runInit();
    Map<String, String> offsets = new HashMap<>();
    final List<Record> records = Collections.synchronizedList(new ArrayList<>());
    // Each thread should run process() exactly once;
    runner.runProduce(offsets, scriptConf.batchSize, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.addAll(output.getRecords().get("lane"));
          runner.setStop();
        }
      });
    try {
      runner.waitOnProduce();
    } catch (ExecutionException | InterruptedException e) {
      System.out.println(e);
    }
    assertEquals(10000, records.size());
    List<EventRecord> events = runner.getEventRecords();
    List<String> errors = runner.getErrors();
    runner.runDestroy();
  }

  @Test
  public void testAllBindings() throws Exception {
    ScriptSourceConfigBean scriptConf = new ScriptSourceConfigBean();
    scriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    scriptConf.params = new HashMap<>();
    scriptConf.params.put("param1", "value1");
    scriptConf.numThreads = 42;
    scriptConf.batchSize = 4242;
    JythonDSource source = new JythonDSource();
    source.scriptConf = scriptConf;
    Map<String, Object> pipelineConstants = Collections.singletonMap(
        "company",
        "StreamSets"
    );
    PushSourceRunner runner = new PushSourceRunner.Builder(JythonDSource.class, source)
        .addConstants(pipelineConstants)
        .addConfiguration("script", getScript("TestAllBindings.py"))
        .addOutputLane("lane")
        .build();
    runner.runInit();
    Map<String, String> offsets = new HashMap<>();
    offsets.put("coolEntityName", "coolOffsetName");
    final List<Record> records = Collections.synchronizedList(new ArrayList<>());
    // Each thread should run process(entityName, entityValue) exactly once;
    runner.runProduce(offsets, scriptConf.batchSize, new PushSourceRunner.Callback() {
      @Override
      public void processBatch(StageRunner.Output output) {
        records.addAll(output.getRecords().get("lane"));
        runner.setStop();
      }
    });
    runner.waitOnProduce();
    runner.runDestroy();
    assertEquals(2, records.size());
    Record record = records.get(0);

    assertEquals(Field.create(scriptConf.batchSize), record.get("/batchSize"));
    assertEquals(Field.create(scriptConf.numThreads), record.get("/numThreads"));

    Map<String, Field> paramsFieldMap = new HashMap<>();
    for (Map.Entry<String, String> pair : scriptConf.params.entrySet()) {
      paramsFieldMap.put(pair.getKey(), Field.create(pair.getValue()));
    }
    assertEquals(Field.create(paramsFieldMap), record.get("/params"));

    Map<String, Field> offsetsFieldMap = new HashMap<>();
    for (Map.Entry<String, String> pair : offsets.entrySet()) {
      offsetsFieldMap.put(pair.getKey(), Field.create(pair.getValue()));
    }
    assertEquals(Field.create(offsetsFieldMap), record.get("/lastOffsets"));

    assertEquals(Field.create(false), record.get("/isStopped()"));

    Map<String, Field> pipelineConstantsFieldMap = new HashMap<>();
    for (Map.Entry<String, Object> pair : pipelineConstants.entrySet()) {
      pipelineConstantsFieldMap.put(
          pair.getKey(),
          Field.create(Field.Type.STRING, pair.getValue(), null)
      );
    }
    assertEquals(
        Field.create(Field.Type.MAP, pipelineConstantsFieldMap, null),
        record.get("/pipelineParameters()")
    );
    assertEquals(Field.create(false), record.get("/isPreview()"));
    assertEquals(
        Field.create(Field.Type.MAP, new HashMap<String, Field>(), null),
        record.get("/createMap()")
    );
    assertEquals(
        Field.create(
            Field.Type.LIST_MAP,
            new ArrayList<HashMap<String, Field>>(),
            null
        ),
        record.get("/createListMap()")
    );
    assertEquals(
        Field.create(Field.Type.BOOLEAN, false, null),
        record.get("/getFieldNull()-false")
    );
    assertEquals(
        Field.create(Field.Type.STRING, null, null),
        record.get("/getFieldNull()-null")
    );
  }

  @Test
  public void testMultiThreadedOffset() throws Exception {
    ScriptSourceConfigBean scriptConf = new ScriptSourceConfigBean();
    scriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    scriptConf.numThreads = 4;
    scriptConf.params = new HashMap<>();
    scriptConf.batchSize = 100;
    JythonDSource source = new JythonDSource();
    source.scriptConf = scriptConf;
    final PushSourceRunner.Builder builder = new PushSourceRunner.Builder(JythonDSource.class, source)
        .addConfiguration("script", getScript("MultiGeneratorOriginScript.py"))
        .addOutputLane("lane");
    final PushSourceRunner runner = builder.build();
    runner.runInit();
    final Map<String, String> offsets = Collections.synchronizedMap(new HashMap<>());
    final List<Record> records = Collections.synchronizedList(new ArrayList<>());
    // Each thread should run process(entityName, entityValue) exactly once;
    runner.runProduce(offsets, scriptConf.batchSize, new PushSourceRunner.Callback() {
      @Override
      public void processBatch(StageRunner.Output output) {
        records.addAll(output.getRecords().get("lane"));
        runner.setStop();
        offsets.put(output.getOffsetEntity(), output.getNewOffset());
      }
    });
    runner.waitOnProduce();
    assertEquals("100", offsets.get("0"));
    assertEquals("100", offsets.get("1"));
    assertEquals("100", offsets.get("2"));
    assertEquals("100", offsets.get("3"));
    runner.runProduce(offsets, scriptConf.batchSize, new PushSourceRunner.Callback() {
      @Override
      public void processBatch(StageRunner.Output output) {
        records.addAll(output.getRecords().get("lane"));
        runner.setStop();
        offsets.replace(output.getOffsetEntity(), output.getNewOffset());
      }
    });
    runner.waitOnProduce();
    assertEquals("200", offsets.get("0"));
    assertEquals("200", offsets.get("1"));
    assertEquals("200", offsets.get("2"));
    assertEquals("200", offsets.get("3"));
    runner.runDestroy();
  }

  @Test
  public void testGetSourceResponseRecords() throws Exception {
  }

  @Test
  public void testHTTPServerAndClient() throws Exception {
    // Set up an HTTP server pipeline
    ScriptSourceConfigBean serverScriptConf = new ScriptSourceConfigBean();
    serverScriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    serverScriptConf.params = new HashMap<>();
    serverScriptConf.numThreads = 1;
    serverScriptConf.batchSize = 1;
    JythonDSource serverSource = new JythonDSource();
    serverSource.scriptConf = serverScriptConf;
    PushSourceRunner serverRunner = new PushSourceRunner.Builder(JythonDSource.class, serverSource)
        .addConfiguration("script", getScript("HTTPServerScript.py"))
        .addOutputLane("lane")
        .build();
    serverRunner.runInit();
    Map<String, String> serverOffsets = new HashMap<>();
    final List<Record> serverRecords = Collections.synchronizedList(new ArrayList<>());
    serverRunner.runProduce(serverOffsets, 1000, new PushSourceRunner.Callback() {
      @Override
      public void processBatch(StageRunner.Output output) {
        serverRecords.addAll(output.getRecords().get("lane"));
        SourceResponseSink response = SinkUtils.createSourceResponseSink();
        Record record = RecordCreator.create();
        record.set(Field.create("You're doing great, kid"));
        response.addResponse(record);
        serverRunner.setSourceResponseSink(response);
      }
    });

    // Set up client pipeline to read from server
    ScriptSourceConfigBean clientScriptConf = new ScriptSourceConfigBean();
    clientScriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    clientScriptConf.params = new HashMap<>();
    clientScriptConf.params.put("baseURL", "//localhost:8080");
    clientScriptConf.numThreads = 10;
    clientScriptConf.batchSize = 100;
    JythonDSource clientSource = new JythonDSource();
    clientSource.scriptConf = clientScriptConf;
    PushSourceRunner clientRunner = new PushSourceRunner.Builder(JythonDSource.class, clientSource)
        .addConfiguration("script", getScript("HTTPClientScript.py"))
        .addOutputLane("lane")
        .build();
    clientRunner.runInit();
    Map<String, String> clientOffsets = new HashMap<>();
    final List<Record> clientRecords = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger nRead = new AtomicInteger(0);
    clientRunner.runProduce(clientOffsets, 1000, new PushSourceRunner.Callback() {
      @Override
      public void processBatch(StageRunner.Output output) {
        clientRecords.addAll(output.getRecords().get("lane"));
        clientRunner.setStop();
        if (nRead.incrementAndGet() >= 10000) {
          clientRunner.setStop();
          serverRunner.setStop();
        }
      }
    });
    clientRunner.waitOnProduce();
    clientRunner.runDestroy();
    serverRunner.runDestroy();
    System.out.println(clientRecords.size());
    for (int i = 0; i < Math.min(clientRecords.size(), 5); i++) {
      System.out.println(clientRecords.get(i));
    }
  }

  @Test
  public void testNullTypes() throws Exception {
    ScriptSourceConfigBean scriptConf = new ScriptSourceConfigBean();
    scriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    scriptConf.params = new HashMap<>();
    scriptConf.numThreads = 1;
    scriptConf.batchSize = 1;
    JythonDSource source = new JythonDSource();
    source.scriptConf = scriptConf;
    PushSourceRunner runner = new PushSourceRunner.Builder(JythonDSource.class, source)
        .addConfiguration("script", getScript("TestNullTypes.py"))
        .addOutputLane("lane")
        .build();
    runner.runInit();
    Map<String, String> offsets = new HashMap<>();
    final List<Record> records = Collections.synchronizedList(new ArrayList<>());
    runner.runProduce(offsets, 1000, new PushSourceRunner.Callback() {
      @Override
      public void processBatch(StageRunner.Output output) {
        records.addAll(output.getRecords().get("lane"));
        runner.setStop();
      }
    });
    runner.waitOnProduce();
    assertEquals(1, records.size());
    Record record = records.get(0);
    assertEquals(
        Field.create(Field.Type.BOOLEAN, null, null),
        record.get("/null_boolean")
    );
    assertEquals(
        Field.create(Field.Type.CHAR, null, null),
        record.get("/null_char")
    );
    assertEquals(
        Field.create(Field.Type.BYTE, null, null),
        record.get("/null_byte")
    );
    assertEquals(
        Field.create(Field.Type.SHORT, null, null),
        record.get("/null_short")
    );
    assertEquals(
        Field.create(Field.Type.INTEGER, null, null),
        record.get("/null_integer")
    );
    assertEquals(
        Field.create(Field.Type.LONG, null, null),
        record.get("/null_long")
    );
    assertEquals(
        Field.create(Field.Type.FLOAT, null, null),
        record.get("/null_float")
    );
    assertEquals(
        Field.create(Field.Type.DOUBLE, null, null),
        record.get("/null_double")
    );
    assertEquals(
        Field.create(Field.Type.DATE, null, null),
        record.get("/null_date")
    );
    assertEquals(
        Field.create(Field.Type.DATETIME, null, null),
        record.get("/null_datetime")
    );
    assertEquals(
        Field.create(Field.Type.TIME, null, null),
        record.get("/null_time")
    );
    assertEquals(
        Field.create(Field.Type.DECIMAL, null, null),
        record.get("/null_decimal")
    );
    assertEquals(
        Field.create(Field.Type.BYTE_ARRAY, null, null),
        record.get("/null_byte_array")
    );
    assertEquals(
        Field.create(Field.Type.STRING, null, null),
        record.get("/null_string")
    );
    assertEquals(
        Field.create(Field.Type.LIST, null, null),
        record.get("/null_list")
    );
    assertEquals(
        Field.create(Field.Type.MAP, null, null),
        record.get("/null_map")
    );
  }
}
