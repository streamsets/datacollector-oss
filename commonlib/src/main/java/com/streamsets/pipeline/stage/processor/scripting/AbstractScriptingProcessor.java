/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.scripting;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.impl.Utils;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractScriptingProcessor extends SingleLaneProcessor {


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

  public AbstractScriptingProcessor(String scriptingEngineName, String scriptConfigGroup, String scriptConfigName,
      ProcessingMode processingMode, String script) {
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
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();
    try {
      engine = new ScriptEngineManager(getClass().getClassLoader()).getEngineByName(scriptingEngineName);
      if (engine == null) {
        issues.add(getContext().createConfigIssue(null, null, Errors.SCRIPTING_00, scriptingEngineName));
      }
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(null, null, Errors.SCRIPTING_01, scriptingEngineName, ex.getMessage(),
                                                ex));
    }

    if (script.trim().isEmpty()) {
      issues.add(getContext().createConfigIssue(scriptConfigGroup, scriptConfigName, Errors.SCRIPTING_02));
    }

    // we cannot verify the script is syntactically correct as there is no way to differentiate
    // that from an exception due to a script execution :(

    return issues;
  }

  @Override
  protected void init() throws StageException {
    super.init();
    err = new Err();
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
          runScript(records, out, err);
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
        runScript(records, out, err);
        break;
      }
    }
  }

  private void runScript(List<ScriptRecord> records, Out out, Err err) throws StageException {
    SimpleBindings bindings = new SimpleBindings();
    bindings.put("records", records.toArray(new Object[records.size()]));
    bindings.put("out", out);
    bindings.put("err", err);
    try {
      runScript(bindings);
    } catch (ScriptException ex) {
      switch (processingMode) {
        case RECORD:
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              getContext().toError(getScriptObjectFactory().getRecord(records.get(0)), Errors.SCRIPTING_05, ex.getMessage(),
                                   ex);
              break;
            case STOP_PIPELINE:
              throw new StageException(Errors.SCRIPTING_05, ex.getMessage(), ex);
            default:
              throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                                                           getContext().getOnErrorRecord(), ex));
          }
          break;
        case BATCH:
          throw new StageException(Errors.SCRIPTING_06, ex.getMessage(), ex);
        default:
          throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                                                       getContext().getOnErrorRecord(), ex));
      }
    }

  }

  private void runScript(SimpleBindings bindings) throws ScriptException {
    engine.eval(script, bindings);
  }

}
