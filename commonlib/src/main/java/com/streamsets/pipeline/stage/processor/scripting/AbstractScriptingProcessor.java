/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.scripting;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
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
    public void write(Object record);
  }

  // to hide all other methods of Stage.Context
  public class Err {
    public void write(Object record, String errMsg) {
      getContext().toError(scriptObjectFactory.getRecord(record), Errors.SCRIPTING_04, errMsg);
    }
  }

  private final String scriptingEngineName;
  private final String scriptConfigGroup;
  private final String scriptConfigName;
  private final ProcessingMode processingMode;
  private final  String script;
  private final ScriptObjectFactory scriptObjectFactory;
  private ScriptEngine engine;
  private Err err;

  public AbstractScriptingProcessor(String scriptingEngineName, String scriptConfigGroup, String scriptConfigName,
      ProcessingMode processingMode, String script) {
    this.scriptingEngineName = scriptingEngineName;
    this.scriptConfigGroup = scriptConfigGroup;
    this.scriptConfigName = scriptConfigName;
    this.processingMode = processingMode;
    this.script = prepareScript(script);
    scriptObjectFactory = getScriptObjectFactory();
  }

  protected ScriptObjectFactory getScriptObjectFactory() {
    return new ScriptObjectFactory();
  }

  protected Object createScriptType() {
    return Field.Type.class;
  }

  protected String prepareScript(String script) {
    return script;
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
      public void write(Object record) {
        singleLaneBatchMaker.addRecord(scriptObjectFactory.getRecord(record));
      }
    };
    switch (processingMode) {
      case RECORD: {
        List records = new ArrayList();
        records.add(null);
        Iterator<Record> it = batch.getRecords();
        while (it.hasNext()) {
          records.set(0, scriptObjectFactory.createRecord(it.next()));
          runScript(records, out, err);
        }
        break;
      }
      case BATCH: {
        List records = new ArrayList();
        Iterator<Record> it = batch.getRecords();
        while (it.hasNext()) {
          records.add(scriptObjectFactory.createRecord(it.next()));
        }
        runScript(records, out, err);
        break;
      }
    }
  }

  protected void addBindings(SimpleBindings bindings) {
  }

  private void runScript(List<Object> records, Out out, Err err) throws StageException {
    SimpleBindings bindings = new SimpleBindings();
    bindings.put("Type", createScriptType());
    bindings.put("records", records.toArray(new Object[records.size()]));
    bindings.put("out", out);
    bindings.put("err", err);
    addBindings(bindings);
    try {
      runScript(bindings);
    } catch (ScriptException ex) {
      switch (processingMode) {
        case RECORD:
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              getContext().toError(scriptObjectFactory.getRecord(records.get(0)), Errors.SCRIPTING_05, ex.getMessage(),
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
