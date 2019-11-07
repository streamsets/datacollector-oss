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
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.util.scripting.DeprecatedBindings;
import com.streamsets.pipeline.stage.util.scripting.Errors;
import com.streamsets.pipeline.stage.util.scripting.ScriptObjectFactory;
import com.streamsets.pipeline.stage.util.scripting.ScriptRecord;
import com.streamsets.pipeline.stage.util.scripting.ScriptStageUtil;
import org.slf4j.Logger;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.streamsets.pipeline.stage.util.scripting.DeprecatedBindings.allDeprecatedMappings;

public abstract class AbstractScriptingProcessor extends SingleLaneProcessor {
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
  private List<ScriptRecord> records;

  protected ScriptEngine engine;
  public final Map<String, String> userParams;


  public ConcurrentHashMap<String, String> unwarnedDeprecatedMappings = new ConcurrentHashMap<>(allDeprecatedMappings);

  public AbstractScriptingProcessor(
      Logger log,
      String scriptingEngineName,
      String scriptConfigGroup,
      ProcessingMode processingMode,
      String script,
      String initScript,
      String destroyScript,
      Map<String, String> userParams
  ) {
    this.log = log;
    this.scriptingEngineName = scriptingEngineName;
    this.scriptConfigGroup = scriptConfigGroup;
    this.processingMode = processingMode;
    this.script = script;
    this.initScript = initScript;
    this.destroyScript = destroyScript;
    this.userParams = userParams;
    this.records = new ArrayList<>();
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

    // We need Stage.Context for createScriptObjectFactory()
    state = getScriptObjectFactory().createMap(false);

    // We want to throw a warning for using deprecated script bindings only once per binding.
    // When pipeline is restarted, we can forget which warnings we've already thrown and throw them all again.
    unwarnedDeprecatedMappings = new ConcurrentHashMap<>(allDeprecatedMappings);

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

    try {
      engine.eval(initScript, createInitDestroyBindings());
    } catch (ScriptException e) {
      issues.add(getContext().createConfigIssue(scriptConfigGroup, "initScript", Errors.SCRIPTING_08, e.toString(), e));
    }

    return issues;
  }

  @Override
  public void destroy() {
    try {
      if(engine != null) {
        engine.eval(destroyScript, createInitDestroyBindings());
      }
    } catch (ScriptException e) {
      log.error(Errors.SCRIPTING_09.getMessage(), e.toString(), e);
    }
    ScriptStageUtil.closeEngine(engine, getInfo(), log);
    engine = null;
    super.destroy();
  }

  @Override
  public void process(Batch batch, final SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    ScriptingProcessorOutput out =
        scriptRecord -> singleLaneBatchMaker.addRecord(getScriptObjectFactory().getRecord(scriptRecord));
    records.clear();

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

  private void runRecord(Batch batch, ScriptingProcessorOutput out) throws StageException {
    records.add(null);
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      records.set(0, getScriptObjectFactory().createScriptRecord(record));
      runScript(records, out);
    }
  }

  private void runBatch(Batch batch, ScriptingProcessorOutput out) throws StageException {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      records.add(getScriptObjectFactory().createScriptRecord(record));
    }
    runScript(records, out);
  }

  private void runScript(List<ScriptRecord> records, ScriptingProcessorOutput out) throws StageException {
    try {
      runScript(createProcessBindings(records, out));
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

  /**
   * This object wraps the ScriptingProcessorInitDestroyBindings to mimic old script bindings.
   * It will be removed in a future release.
   */
  @Deprecated
  public class SdcFunctions {
    private final ScriptingProcessorInitDestroyBindings spb;

    public SdcFunctions(ScriptingProcessorInitDestroyBindings spb) {
      this.spb = spb;
    }

    public ScriptRecord createRecord(String recordSourceId) {
      return spb.createRecord(recordSourceId);
    }

    public ScriptRecord createEvent(String type, int version) {
      return spb.createEvent(type, version);
    }

    public void toEvent(ScriptRecord event) throws StageException {
      spb.toEvent(event);
    }

    public boolean isPreview() {
      return spb.isPreview();
    }

    public boolean isStopped() {
      return spb.isStopped();
    }

    public Map<String, Object> pipelineParameters() {
      return spb.pipelineParameters();
    }

    public Object createMap(boolean listMap) {
      return spb.createMap(listMap);
    }

    public Object getFieldNull(ScriptRecord scriptRecord, String fieldPath) {
      return spb.getFieldNull(scriptRecord, fieldPath);
    }
  }


  @Deprecated
  private void addDeprecatedInitDestroyBindings(DeprecatedBindings bindings,
                                                ScriptingProcessorInitDestroyBindings spb) {
    bindings.put("error", spb.error);
    bindings.put("log", spb.log);
    bindings.put("state", spb.state);
    bindings.put("sdcFunctions", new SdcFunctions(spb));
    bindings.put("NULL_BOOLEAN", spb.NULL_BOOLEAN);
    bindings.put("NULL_CHAR", spb.NULL_CHAR);
    bindings.put("NULL_BYTE", spb.NULL_BYTE);
    bindings.put("NULL_SHORT", spb.NULL_SHORT);
    bindings.put("NULL_INTEGER", spb.NULL_INTEGER);
    bindings.put("NULL_LONG", spb.NULL_LONG);
    bindings.put("NULL_FLOAT", spb.NULL_FLOAT);
    bindings.put("NULL_DOUBLE",spb.NULL_DOUBLE);
    bindings.put("NULL_DATE", spb.NULL_DATE);
    bindings.put("NULL_DATETIME", spb.NULL_DATETIME);
    bindings.put("NULL_TIME",spb.NULL_TIME);
    bindings.put("NULL_DECIMAL", spb.NULL_DECIMAL);
    bindings.put("NULL_BYTE_ARRAY", spb.NULL_BYTE_ARRAY);
    bindings.put("NULL_STRING", spb.NULL_STRING);
    bindings.put("NULL_LIST", spb.NULL_LIST);
    bindings.put("NULL_MAP", spb.NULL_MAP);
  }

  @Deprecated
  // Bindings available in a "process" script are a superset of those available in "init" or "destroy" scripts
  private void addDeprecatedProcessBindings(DeprecatedBindings bindings,
                                            ScriptingProcessorProcessBindings spb) {
    addDeprecatedInitDestroyBindings(bindings, spb);
    bindings.put("records", spb.records);
    bindings.put("output", spb.output);
  }

  private SimpleBindings createInitDestroyBindings() {
    DeprecatedBindings bindings = new DeprecatedBindings(log, unwarnedDeprecatedMappings);

    // Add new bindings
    ScriptingProcessorInitDestroyBindings spb = new ScriptingProcessorInitDestroyBindings(
        scriptObjectFactory,
        getContext(),
        errorRecordHandler,

        userParams,
        log,
        state
    );
    bindings.put("sdc", spb);

    // Add deprecated bindings
    addDeprecatedInitDestroyBindings(bindings, spb);

    return bindings;
  }

  private SimpleBindings createProcessBindings(List<ScriptRecord> records, ScriptingProcessorOutput out) {
    DeprecatedBindings bindings = new DeprecatedBindings(log, unwarnedDeprecatedMappings);

    // Add new bindings
    ScriptingProcessorProcessBindings spb = new ScriptingProcessorProcessBindings(
        scriptObjectFactory, getContext(), errorRecordHandler, userParams, log, state,
        out, records);
    bindings.put("sdc", spb);

    // Add deprecated bindings
    addDeprecatedProcessBindings(bindings, spb);

    return bindings;
  }

  private void runScript(SimpleBindings bindings) throws ScriptException {
    compiledScript.eval(bindings);
  }

  public List<ScriptRecord> getScriptRecords() {
    return records;
  }
}
