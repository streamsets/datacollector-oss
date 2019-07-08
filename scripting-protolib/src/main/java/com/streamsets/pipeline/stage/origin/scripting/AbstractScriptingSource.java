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

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToEventContext;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.origin.scripting.config.ScriptSourceConfigBean;

import org.slf4j.Logger;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class AbstractScriptingSource extends BasePushSource {
  private static final String ERROR_BINDING_NAME = "sdcError";
  private static final String LOG_BINDING_NAME = "sdcLog";
  private static final String PARAMS_BINDING_NAME = "sdcUserParams";
  private Logger log;
  private String scriptingEngineName;
  private String scriptConfigGroup;

  private final String script;
  private final Map<String, String> params;
  private final int numThreads;
  private final int batchSize;

  private CompiledScript compiledScript;
  private ScriptObjectFactory scriptObjectFactory;
  private ErrorRecordHandler errorRecordHandler;
  private Err err;
  private SdcFunctions sdcFunc;

  protected ScriptEngine engine;

  public class PushSourceScriptBatch {
    private int batchSize;
    private BatchContext batchContext;
    private String[] allLanes;

    PushSourceScriptBatch () {
      batchContext = getContext().startBatch();
      batchSize = 0;
      allLanes = batchContext.getBatchMaker().getLanes().toArray(new String[0]);
    }

    public int size() {
      return batchSize;
    }

    public boolean process(String entityName, String entityOffset) {
      if (entityName == null) {
        entityName = "";
      }
      batchSize = 0;
      PushSource.Context context = getContext();
      return context.processBatch(batchContext, entityName, entityOffset);
    }

    public void add(ScriptRecord scriptRecord) {
      batchSize++;
      Record record = getScriptObjectFactory().getRecord(scriptRecord);
      batchContext.getBatchMaker().addRecord(record, allLanes);
    }

    // TODO: untested by Jython scripting origin
    public void add(Collection<ScriptRecord> scriptRecords) {
      for (ScriptRecord scriptRecord : scriptRecords) {
        add(scriptRecord);
      }
    }

    // a Jython list of records hits this method signature;
    public void add(ScriptRecord[] scriptRecords) {
      for (ScriptRecord scriptRecord : scriptRecords) {
        add(scriptRecord);
      }
    }

    public List<ScriptRecord> getSourceResponseRecords() {
      List<Record> records = batchContext.getSourceResponseRecords();
      List<ScriptRecord> scriptRecords = new ArrayList<>();
      for (Record record : records) {
        scriptRecords.add(getScriptObjectFactory().createScriptRecord(record));
      }
      return scriptRecords;
    }
  }

  // to hide all other methods of Stage.Context
  public class Err {
    public void write(ScriptRecord scriptRecord, String errMsg) throws StageException {
      errorRecordHandler.onError(new OnRecordErrorException(
          getScriptObjectFactory().getRecord(scriptRecord),
          Errors.SCRIPTING_04,
          errMsg
      ));
    }
  }

  // This class will contain functions to expose to scripting origins
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
      PushSource.Context context = getContext();
      EventRecord er = context.createEventRecord(type, version, recordSourceId);
      ScriptObjectFactory sof = getScriptObjectFactory();
      ScriptRecord sr = sof.createScriptRecord(er);
      return sr;
    }

    public void toEvent(ScriptRecord event) throws StageException {
      Record eventRecord = null;
      if(event instanceof SdcScriptRecord) {
        eventRecord = ((SdcScriptRecord) event).sdcRecord;
      } else {
        eventRecord = ((NativeScriptRecord) event).sdcRecord;
      }

      if(!(eventRecord instanceof EventRecord)) {
        log.error("Can't send normal record to event stream: {}", eventRecord);
        throw new StageException(Errors.SCRIPTING_07, eventRecord.getHeader().getSourceId());
      }

      ((ToEventContext) getContext()).toEvent((EventRecord)getScriptObjectFactory().getRecord(event));
    }

    public boolean isPreview() { return getContext().isPreview(); }

    public Object createMap(boolean listMap) {
      return getScriptObjectFactory().createMap(listMap);
    }

    public Map<String, Object> pipelineParameters() {
      return getContext().getPipelineConstants();
    }

    public PushSourceScriptBatch createBatch() {
      return new PushSourceScriptBatch();
    }

    public boolean isStopped() {
      return getContext().isStopped();
    }
  }

  public AbstractScriptingSource(
      Logger log,
      String scriptingEngineName,
      String scriptConfigGroup,
      String script,
      ScriptSourceConfigBean scriptConf
  ) {
    this.log = log;
    this.scriptingEngineName = scriptingEngineName;
    this.scriptConfigGroup = scriptConfigGroup;
    this.script = script;
    batchSize = scriptConf.batchSize;
    numThreads = scriptConf.numThreads;
    params = scriptConf.params;
  }

  @Override
  public int getNumberOfThreads() {
    return numThreads;
  }

  private ScriptObjectFactory getScriptObjectFactory() {
    if (scriptObjectFactory == null) {
      scriptObjectFactory = createScriptObjectFactory(getContext());
    }
    return scriptObjectFactory;
  }

  protected abstract ScriptObjectFactory createScriptObjectFactory(Stage.Context context);

  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler((Source.Context) getContext());

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

    return issues;
  }

  private SimpleBindings createBindings(Map<String, String> lastOffsets, int maxBatchSize) {
    SimpleBindings bindings = new SimpleBindings();

    bindings.put(ERROR_BINDING_NAME, err);
    bindings.put(PARAMS_BINDING_NAME, params);
    bindings.put(LOG_BINDING_NAME, log);
    bindings.put("sdcConstants.numThreads", numThreads);
    bindings.put("sdcConstants.batchSize", Math.min(batchSize, maxBatchSize));
    bindings.put("sdcConstants.lastOffsets", lastOffsets);
    ScriptTypedNullObject.fillNullTypes(bindings);
    bindings.put("sdcFunctions", sdcFunc);

    return bindings;
  }

  private void runScript(SimpleBindings bindings) throws ScriptException {
    compiledScript.eval(bindings);
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
    try {
      runScript(createBindings(lastOffsets, maxBatchSize));
    } catch (ScriptException e) {
      throw new StageException(Errors.SCRIPTING_10, e.toString());
    }
  }

  @Override
  public void destroy() {
    super.destroy();
  }

}
