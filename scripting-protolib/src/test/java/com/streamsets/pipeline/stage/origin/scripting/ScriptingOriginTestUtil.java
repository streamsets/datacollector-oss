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
package com.streamsets.pipeline.stage.origin.scripting;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.SourceResponseSink;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SinkUtils;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.scripting.config.ScriptSourceConfigBean;
import com.streamsets.pipeline.stage.util.scripting.config.ScriptRecordType;

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

/**
 * Common unit tests for scripting origins
 */
public class ScriptingOriginTestUtil<T extends AbstractScriptingSource> {

  public static String getScript(String scriptName, Class searchClass) {
    try {
      URL url = Resources.getResource(searchClass, scriptName);
      return Resources.toString(url, Charsets.UTF_8);
    } catch (IOException e) {
      System.out.println(e);
      return null;
    }
  }

  public static <C extends AbstractScriptingDSource> void testResumeGenerateRecords(
      Class<C> clazz,
      AbstractScriptingDSource scriptingDSource,
      String scriptName)
      throws Exception {
    ScriptSourceConfigBean scriptConf = new ScriptSourceConfigBean();
    scriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    scriptConf.params = Collections.unmodifiableMap(new HashMap<>());
    scriptConf.numThreads = 1;
    scriptConf.batchSize = 1000;
    scriptingDSource.scriptConf = scriptConf;
    final PushSourceRunner runner = new PushSourceRunner.Builder(clazz, scriptingDSource)
        .addConfiguration("script", getScript(scriptName, clazz))
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    runner.runInit();
    Map<String, String> oldOffsets = new HashMap<>();
    oldOffsets.put("", "1000");
    Map<String, String> offsets = Collections.unmodifiableMap(new HashMap<>(oldOffsets));
    Map<String, String> newOffsets = new HashMap<>();
    final List<Record> records = Collections.synchronizedList(new ArrayList<>());
    runner.runProduce(offsets, scriptConf.batchSize, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.addAll(output.getRecords().get("lane"));
          runner.setStop();
          newOffsets.put(output.getOffsetEntity(), output.getNewOffset());
        }
      });
    runner.waitOnProduce();
    assertEquals("2000", newOffsets.get(""));
    assertEquals(0, runner.getEventRecords().size());
    assertEquals(0, runner.getErrorRecords().size());
    assertEquals(1000, records.size());
    assertEquals(":2000", records.get(999).get("").getValueAsString());
    runner.runDestroy();
  }

  public static <C extends AbstractScriptingDSource> void testMultiThreadGenerateRecords(
      Class<C> clazz,
      AbstractScriptingDSource scriptingDSource,
      String scriptName)
      throws Exception {
    ScriptSourceConfigBean scriptConf = new ScriptSourceConfigBean();
    scriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    scriptConf.params = Collections.unmodifiableMap(new HashMap<>());
    scriptConf.numThreads = 10;
    scriptConf.batchSize = 1000;
    scriptingDSource.scriptConf = scriptConf;
    PushSourceRunner runner = new PushSourceRunner.Builder(clazz, scriptingDSource)
        .addConfiguration("script", getScript(scriptName, clazz))
        .addOutputLane("lane")
        .build();
    runner.runInit();
    Map<String, String> offsets = Collections.unmodifiableMap(new HashMap<>());
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

  public static <C extends AbstractScriptingDSource> void testAllBindings(
      Class<C> clazz,
      AbstractScriptingDSource scriptingDSource,
      String scriptName
  ) throws Exception {
    ScriptSourceConfigBean scriptConf = new ScriptSourceConfigBean();
    scriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    Map<String, String> params = new HashMap<>();
    params.put("param1", "value1");
    scriptConf.params = Collections.unmodifiableMap(params);
    scriptConf.numThreads = 42;
    scriptConf.batchSize = 4242;
    scriptingDSource.scriptConf = scriptConf;
    Map<String, Object> pipelineConstants = Collections.singletonMap(
        "company",
        "StreamSets"
    );
    PushSourceRunner runner = new PushSourceRunner.Builder(clazz, scriptingDSource)
        .addConstants(pipelineConstants)
        .addConfiguration("script", getScript(scriptName, clazz))
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    Map<String, String> coolOffsets = new HashMap<>();
    coolOffsets.put("coolEntityName", "coolOffsetName");
    Map<String, String> offsets = Collections.unmodifiableMap(coolOffsets);
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
    assertEquals(
        // Note - size() is called before records are added to batch so should be 0 not 2
        Field.create(Field.Type.INTEGER, 0, null),
        record.get("/batch.size()")
    );
    assertEquals(
        Field.create(Field.Type.INTEGER, 1, null),
        record.get("/batch.eventCount()")
    );
    assertEquals(
        Field.create(Field.Type.INTEGER, 1, null),
        record.get("/batch.errorCount()")
    );
  }

  public static <C extends AbstractScriptingDSource> void testMultiThreadedOffset(
      Class<C> clazz,
      AbstractScriptingDSource scriptingDSource,
      String scriptName
  ) throws Exception {
    ScriptSourceConfigBean scriptConf = new ScriptSourceConfigBean();
    scriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    scriptConf.numThreads = 4;
    scriptConf.params = Collections.unmodifiableMap(new HashMap<>());
    scriptConf.batchSize = 100;
    scriptingDSource.scriptConf = scriptConf;
    final PushSourceRunner.Builder builder = new PushSourceRunner.Builder(clazz, scriptingDSource)
        .addConfiguration("script", getScript(scriptName, clazz))
        .addOutputLane("lane");
    final PushSourceRunner runner = builder.build();
    runner.runInit();
    Map<String, String> offsets = Collections.unmodifiableMap(new HashMap<>());
    Map<String, String> newOffsets = Collections.synchronizedMap(new HashMap<>());
    final List<Record> records = Collections.synchronizedList(new ArrayList<>());
    // Each thread should run process(entityName, entityValue) exactly once;
    runner.runProduce(offsets, scriptConf.batchSize, new PushSourceRunner.Callback() {
      @Override
      public void processBatch(StageRunner.Output output) {
        records.addAll(output.getRecords().get("lane"));
        runner.setStop();
        newOffsets.put(output.getOffsetEntity(), output.getNewOffset());
      }
    });
    runner.waitOnProduce();
    assertEquals("100", newOffsets.get("0"));
    assertEquals("100", newOffsets.get("1"));
    assertEquals("100", newOffsets.get("2"));
    assertEquals("100", newOffsets.get("3"));
    offsets = Collections.unmodifiableMap(new HashMap<>(newOffsets));
    runner.runProduce(offsets, scriptConf.batchSize, new PushSourceRunner.Callback() {
      @Override
      public void processBatch(StageRunner.Output output) {
        records.addAll(output.getRecords().get("lane"));
        runner.setStop();
        newOffsets.replace(output.getOffsetEntity(), output.getNewOffset());
      }
    });
    runner.waitOnProduce();
    assertEquals("200", newOffsets.get("0"));
    assertEquals("200", newOffsets.get("1"));
    assertEquals("200", newOffsets.get("2"));
    assertEquals("200", newOffsets.get("3"));
    runner.runDestroy();
  }

  // Set up an HTTP server pipeline
  public static <C extends AbstractScriptingDSource> void testHTTPServerAndClient(
      Class<C> clazz,
      AbstractScriptingDSource serverDSource,
      AbstractScriptingDSource clientDSource,
      String serverScriptName,
      String clientScriptName
  ) throws Exception {
    ScriptSourceConfigBean serverScriptConf = new ScriptSourceConfigBean();
    serverScriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    serverScriptConf.params = Collections.unmodifiableMap(new HashMap<>());
    serverScriptConf.numThreads = 1;
    serverScriptConf.batchSize = 1;
    serverDSource.scriptConf = serverScriptConf;
    PushSourceRunner serverRunner = new PushSourceRunner.Builder(clazz, serverDSource)
        .addConfiguration("script", getScript(serverScriptName, clazz))
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
    Map<String, String> params = new HashMap<>();
    params.put("baseURL", "//localhost:8080");
    clientScriptConf.params = Collections.unmodifiableMap(params);
    clientScriptConf.numThreads = 10;
    clientScriptConf.batchSize = 100;
    clientDSource.scriptConf = clientScriptConf;
    PushSourceRunner clientRunner = new PushSourceRunner.Builder(clazz, clientDSource)
        .addConfiguration("script", getScript(clientScriptName, clazz))
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

  public static <C extends AbstractScriptingDSource> void testNullTypes(
      Class<C> clazz,
      AbstractScriptingDSource scriptingDSource,
      String scriptName
  ) throws Exception {
    ScriptSourceConfigBean scriptConf = new ScriptSourceConfigBean();
    scriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    scriptConf.params = Collections.unmodifiableMap(new HashMap<>());
    scriptConf.numThreads = 1;
    scriptConf.batchSize = 1;
    scriptingDSource.scriptConf = scriptConf;
    PushSourceRunner runner = new PushSourceRunner.Builder(clazz, scriptingDSource)
        .addConfiguration("script", getScript(scriptName, clazz))
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

  public static <C extends AbstractScriptingDSource> void testGenerateErrorRecords(
      Class<C> clazz,
      AbstractScriptingDSource scriptingDSource,
      String scriptName)
      throws Exception {
    ScriptSourceConfigBean scriptConf = new ScriptSourceConfigBean();
    scriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    scriptConf.params = Collections.unmodifiableMap(new HashMap<>());
    scriptConf.numThreads = 1;
    scriptConf.batchSize = 23;
    scriptingDSource.scriptConf = scriptConf;
    final PushSourceRunner runner = new PushSourceRunner.Builder(clazz, scriptingDSource)
        .addConfiguration("script", getScript(scriptName, clazz))
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    Map<String, String> offsets = Collections.unmodifiableMap(new HashMap<>());
    final List<Record> records = Collections.synchronizedList(new ArrayList<>());
    runner.runProduce(offsets, scriptConf.batchSize, new PushSourceRunner.Callback() {
      @Override
      public void processBatch(StageRunner.Output output) {
        records.addAll(output.getRecords().get("lane"));
        runner.setStop();
      }
    });
    runner.waitOnProduce();
    assertEquals(scriptConf.batchSize, runner.getErrorRecords().size());
    runner.runDestroy();
  }

  public static <C extends AbstractScriptingDSource> void testGenerateEvents(
      Class<C> clazz,
      AbstractScriptingDSource scriptingDSource,
      String scriptName)
      throws Exception {
    ScriptSourceConfigBean scriptConf = new ScriptSourceConfigBean();
    scriptConf.scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;
    scriptConf.params = Collections.unmodifiableMap(new HashMap<>());
    scriptConf.numThreads = 1;
    scriptConf.batchSize = 19;
    scriptingDSource.scriptConf = scriptConf;
    final PushSourceRunner runner = new PushSourceRunner.Builder(clazz, scriptingDSource)
        .addConfiguration("script", getScript(scriptName, clazz))
        .addOutputLane("lane")
        .build();
    runner.runInit();
    Map<String, String> offsets = Collections.unmodifiableMap(new HashMap<>());
    final List<Record> records = Collections.synchronizedList(new ArrayList<>());
    runner.runProduce(offsets, scriptConf.batchSize, new PushSourceRunner.Callback() {
      @Override
      public void processBatch(StageRunner.Output output) {
        records.addAll(output.getRecords().get("lane"));
        runner.setStop();
      }
    });
    runner.waitOnProduce();
    assertEquals(scriptConf.batchSize, runner.getEventRecords().size());
    runner.runDestroy();
  }

}
