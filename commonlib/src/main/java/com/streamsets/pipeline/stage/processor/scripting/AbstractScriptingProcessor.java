/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.pipeline.api.Field;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

public abstract class AbstractScriptingProcessor extends SingleLaneProcessor {
  private final Logger log;

  // to hide all other methods of batchMaker
  public interface Out {
    public void write(ScriptRecord record);
  }

  // to hide all other methods of Stage.Context
  public class Err {
    public void write(ScriptRecord scriptRecord, String errMsg) {
      getContext().toError(getScriptObjectFactory().getRecord(scriptRecord), Errors.SCRIPTING_04, errMsg);
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

  }

  private final String scriptingEngineName;
  private final String scriptConfigGroup;
  private final String scriptConfigName;
  private final ProcessingMode processingMode;
  private final  String script;
  private final SimpleBindings bindings = new SimpleBindings();
  // State obj for use by end-user scripts.
  private Object state;

  private CompiledScript compiledScript;
  private ScriptObjectFactory scriptObjectFactory;
  private ErrorRecordHandler errorRecordHandler;
  private Err err;
  private SdcFunctions sdcFunc;

  protected ScriptEngine engine;

  public AbstractScriptingProcessor(
      Logger log,
      String scriptingEngineName,
      String scriptConfigGroup,
      String scriptConfigName,
      ProcessingMode processingMode,
      String script
  ) {
    this.log = log;
    this.scriptingEngineName = scriptingEngineName;
    this.scriptConfigGroup = scriptConfigGroup;
    this.scriptConfigName = scriptConfigName;
    this.processingMode = processingMode;
    this.script = script;
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
      issues.add(getContext().createConfigIssue(scriptConfigGroup, scriptConfigName, Errors.SCRIPTING_02));
    } else {
      try {
        compiledScript = ((Compilable) engine).compile(script);
      } catch (ScriptException e) {
        // This likely means that there is a syntactic error in the script.
        issues.add(
            getContext().createConfigIssue(scriptConfigGroup, scriptConfigName, Errors.SCRIPTING_03, e.toString())
        );
        log.error(Errors.SCRIPTING_03.getMessage(), e.toString(), e);
      }
    }

    err = new Err();
    sdcFunc = new SdcFunctions();
    return issues;
  }

  @Override
  public void process(Batch batch, final SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    Out out = new Out() {
      @Override
      public void write(ScriptRecord scriptRecord) {
        singleLaneBatchMaker.addRecord(getScriptObjectFactory().getRecord(scriptRecord));
      }
    };

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
      runScript(records, out, err, state, log);
    }
  }

  private void runBatch(Batch batch, Out out) throws StageException {
    List<ScriptRecord> records = new ArrayList<>();
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      records.add(getScriptObjectFactory().createScriptRecord(record));
    }
    runScript(records, out, err, state, log);
  }

  private void runScript(List<ScriptRecord> records, Out out, Err err, Object state, Logger log) throws StageException {
    bindings.put("records", records.toArray(new Object[records.size()]));
    bindings.put("output", out);
    bindings.put("error", err);
    bindings.put("state", state);
    bindings.put("log", log);
    ScriptTypedNullObject.fillNullTypes(bindings);
    bindings.put("sdcFunctions", sdcFunc);

    try {
      runScript(bindings);
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

  private void runScript(SimpleBindings bindings) throws ScriptException {
    compiledScript.eval(bindings);
  }

}
