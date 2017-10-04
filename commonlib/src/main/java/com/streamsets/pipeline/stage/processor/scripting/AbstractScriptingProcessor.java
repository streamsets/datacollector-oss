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

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class AbstractScriptingProcessor extends SingleLaneProcessor {
  private static final String STATE_BINDING_NAME = "state";
  private static final String LOG_BINDING_NAME = "log";
  private final Logger log;

  private final String scriptingEngineName;
  private final String scriptConfigGroup;
  private final ProcessingMode processingMode;
  private final String script;
  private final String initScript;
  private final String destroyScript;
  // State obj for use by end-user scripts.
  private Object state;

  private CompiledScript compiledScript;
  private ScriptObjectFactory scriptObjectFactory;
  private ErrorRecordHandler errorRecordHandler;
  private Err err;
  private SdcFunctions sdcFunc;

  protected ScriptEngine engine;

  // to hide all other methods of batchMaker
  public interface Out {
    void write(ScriptRecord record);
  }

  // to hide all other methods of Stage.Context
  public class Err {
    public void write(ScriptRecord scriptRecord, String errMsg) throws StageException {
      errorRecordHandler.onError(new OnRecordErrorException(getScriptObjectFactory().getRecord(scriptRecord), Errors.SCRIPTING_04, errMsg));
    }
  }

  // This class will contain functions to expose to scripting processors
  public class SdcFunctions {

    // To access getFieldNull function through SimpleBindings
    public Object getFieldNull(ScriptRecord scriptRecord, String fieldPath) {
      return ScriptTypedNullObject.getFieldNull(getScriptObjectFactory().getRecord(scriptRecord), fieldPath);
    }

    /**
     * Create record
     * Note: Default field value is null.
     * @param recordSourceId the unique record id for this record.
     * @return ScriptRecord The Newly Created Record
     */
    public ScriptRecord createRecord(String recordSourceId) {
      return getScriptObjectFactory().createScriptRecord(getContext().createRecord(recordSourceId));
    }

    public ScriptRecord createEvent(String type, int version) {
      String recordSourceId = Utils.format("event:{}:{}:{}", type, version, System.currentTimeMillis());
      return getScriptObjectFactory().createScriptRecord(getContext().createEventRecord(type, version, recordSourceId));
    }

    public void toEvent(ScriptRecord event) throws StageException {
      if(!(event.record instanceof EventRecord)) {
        log.error("Can't send normal record to event stream: {}", event.record);
        throw new StageException(Errors.SCRIPTING_07, event.record.getHeader().getSourceId());
      }

      getContext().toEvent((EventRecord)getScriptObjectFactory().getRecord(event));
    }

    public Object createMap(boolean listMap) {
      return getScriptObjectFactory().createMap(listMap);
    }

    public Map<String, Object> pipelineParameters() {
      return getContext().getPipelineConstants();
    }
  }

  public AbstractScriptingProcessor(
      Logger log,
      String scriptingEngineName,
      String scriptConfigGroup,
      ProcessingMode processingMode,
      String script,
      String initScript,
      String destroyScript
  ) {
    this.log = log;
    this.scriptingEngineName = scriptingEngineName;
    this.scriptConfigGroup = scriptConfigGroup;
    this.processingMode = processingMode;
    this.script = script;
    this.initScript = initScript;
    this.destroyScript = destroyScript;
  }

  private ScriptObjectFactory getScriptObjectFactory() {
    if (scriptObjectFactory == null) {
      scriptObjectFactory = createScriptObjectFactory(getContext());
    }
    return scriptObjectFactory;
  }

  protected abstract ScriptObjectFactory createScriptObjectFactory(Stage.Context context);

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    //We need Stage.Context for createScriptObjectFactory()
    state = getScriptObjectFactory().createMap(false);

    try {
      engine = new ScriptEngineManager(getClass().getClassLoader()).getEngineByName(scriptingEngineName);
      if (engine == null) {
        issues.add(getContext().createConfigIssue(null, null, Errors.SCRIPTING_00, scriptingEngineName));
      }
    } catch (Exception ex) {
      issues.add(
          getContext().createConfigIssue(null, null, Errors.SCRIPTING_01, scriptingEngineName, ex.toString(), ex)
      );
    }

    if (script.trim().isEmpty()) {
      issues.add(getContext().createConfigIssue(scriptConfigGroup, "script", Errors.SCRIPTING_02));
    } else {
      try {
        compiledScript = ((Compilable) engine).compile(script);
      } catch (ScriptException e) {
        // This likely means that there is a syntactic error in the script.
        issues.add(
            getContext().createConfigIssue(scriptConfigGroup, "script", Errors.SCRIPTING_03, e.toString())
        );
        log.error(Errors.SCRIPTING_03.getMessage(), e.toString(), e);
      }
    }

    err = new Err();
    sdcFunc = new SdcFunctions();

    try {
      engine.eval(initScript, createBindings());
    } catch (ScriptException e) {
      issues.add(getContext().createConfigIssue(scriptConfigGroup, "initScript", Errors.SCRIPTING_08, e.toString(), e));
    }

    return issues;
  }

  @Override
  public void destroy() {
    try {
      if(engine != null) {
        engine.eval(destroyScript, createBindings());
      }
    } catch (ScriptException e) {
      log.error(Errors.SCRIPTING_09.getMessage(), e.toString(), e);
    }
    super.destroy();
  }

  @Override
  public void process(Batch batch, final SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    Out out = scriptRecord -> singleLaneBatchMaker.addRecord(getScriptObjectFactory().getRecord(scriptRecord));

    switch (processingMode) {
      case RECORD:
        runRecord(batch, out);
        break;
      case BATCH:
        runBatch(batch, out);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unknown Processing Mode: '{}'", processingMode));
    }
  }

  private void runRecord(Batch batch, Out out) throws StageException {
    List<ScriptRecord> records = new ArrayList<>();
    records.add(null);
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      records.set(0, getScriptObjectFactory().createScriptRecord(record));
      runScript(records, out);
    }
  }

  private void runBatch(Batch batch, Out out) throws StageException {
    List<ScriptRecord> records = new ArrayList<>();
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      records.add(getScriptObjectFactory().createScriptRecord(record));
    }
    runScript(records, out);
  }

  private void runScript(List<ScriptRecord> records, Out out) throws StageException {
    try {
      runScript(createBindings(records, out));
    } catch (ScriptException ex) {
      switch (processingMode) {
        case RECORD:
          errorRecordHandler.onError(
              new OnRecordErrorException(
                  getScriptObjectFactory().getRecord(records.get(0)),
                  Errors.SCRIPTING_05,
                  ex.toString(),
                  ex
              )
          );
          break;
        case BATCH:
          throw new StageException(Errors.SCRIPTING_06, ex.toString(), ex);
        default:
          throw new IllegalStateException(
              Utils.format("Unknown OnError value '{}'", getContext().getOnErrorRecord(), ex)
          );
      }
    }
  }

  private SimpleBindings createBindings(List<ScriptRecord> records, Out out) {
    SimpleBindings bindings = createBindings();
    bindings.put("records", records.toArray(new Object[records.size()]));
    bindings.put("output", out);
    return bindings;
  }

  private SimpleBindings createBindings() {
    SimpleBindings bindings = new SimpleBindings();

    bindings.put("error", err);
    bindings.put(STATE_BINDING_NAME, state);
    bindings.put(LOG_BINDING_NAME, log);
    ScriptTypedNullObject.fillNullTypes(bindings);
    bindings.put("sdcFunctions", sdcFunc);

    return bindings;
  }

  private void runScript(SimpleBindings bindings) throws ScriptException {
    compiledScript.eval(bindings);
  }

}
