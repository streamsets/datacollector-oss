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
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.Iterator;
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

  private final String scriptingEngineName;
  private final String scriptConfigGroup;
  private final String scriptConfigName;
  private final ProcessingMode processingMode;
  private final  String script;
  private ScriptObjectFactory scriptObjectFactory;
  protected ScriptEngine engine;
  private Err err;

  // State obj for use by end-user scripts.
  private final Object state;

  public AbstractScriptingProcessor(
      Logger log,
      String scriptingEngineName,
      String scriptConfigGroup,
      String scriptConfigName,
      ProcessingMode processingMode,
      String script
  ) {
    state = getScriptObjectFactory().createMap(false);
    this.log = log;
    this.scriptingEngineName = scriptingEngineName;
    this.scriptConfigGroup = scriptConfigGroup;
    this.scriptConfigName = scriptConfigName;
    this.processingMode = processingMode;
    this.script = script;
  }

  private ScriptObjectFactory getScriptObjectFactory() {
    if (scriptObjectFactory == null) {
      scriptObjectFactory = createScriptObjectFactory();
    }
    return scriptObjectFactory;
  }

  protected ScriptObjectFactory createScriptObjectFactory() {
    return new ScriptObjectFactory(engine);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    try {
      engine = new ScriptEngineManager(getClass().getClassLoader()).getEngineByName(scriptingEngineName);
      if (engine == null) {
        issues.add(getContext().createConfigIssue(null, null, Errors.SCRIPTING_00, scriptingEngineName));
      }
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(null, null, Errors.SCRIPTING_01, scriptingEngineName, ex.toString(),
                                                ex));
    }

    if (script.trim().isEmpty()) {
      issues.add(getContext().createConfigIssue(scriptConfigGroup, scriptConfigName, Errors.SCRIPTING_02));
    }

    // we cannot verify the script is syntactically correct as there is no way to differentiate
    // that from an exception due to a script execution :(

    err = new Err();

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
      case RECORD: {
        List<ScriptRecord> records = new ArrayList<>();
        records.add(null);
        Iterator<Record> it = batch.getRecords();
        while (it.hasNext()) {
          Record record = it.next();
          records.set(0, getScriptObjectFactory().createScriptRecord(record));
          runScript(records, out, err, state, log);
        }
        break;
      }
      case BATCH: {
        List<ScriptRecord> records = new ArrayList<>();
        Iterator<Record> it = batch.getRecords();
        while (it.hasNext()) {
          Record record = it.next();
          records.add(getScriptObjectFactory().createScriptRecord(record));
        }
        runScript(records, out, err, state, log);
        break;
      }
    }
  }

  private void runScript(List<ScriptRecord> records, Out out, Err err, Object state, Logger LOG) throws StageException {
    SimpleBindings bindings = new SimpleBindings();
    bindings.put("records", records.toArray(new Object[records.size()]));
    bindings.put("output", out);
    bindings.put("error", err);
    bindings.put("state", state);
    bindings.put("log", LOG);
    try {
      runScript(bindings);
    } catch (ScriptException ex) {
      switch (processingMode) {
        case RECORD:
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              getContext().toError(
                  getScriptObjectFactory().getRecord(records.get(0)),
                  Errors.SCRIPTING_05,
                  ex.toString(),
                  ex
              );
              break;
            case STOP_PIPELINE:
              throw new StageException(Errors.SCRIPTING_05, ex.toString(), ex);
            default:
              throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
                                                           getContext().getOnErrorRecord(), ex));
          }
          break;
        case BATCH:
          throw new StageException(Errors.SCRIPTING_06, ex.toString(), ex);
        default:
          throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
                                                       getContext().getOnErrorRecord(), ex));
      }
    }

  }

  private void runScript(SimpleBindings bindings) throws ScriptException {
    engine.eval(script, bindings);
  }

}
