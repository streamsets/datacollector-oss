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

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.origin.scripting.config.ScriptSourceConfigBean;

import com.streamsets.pipeline.stage.util.scripting.Errors;
import com.streamsets.pipeline.stage.util.scripting.ScriptObjectFactory;
import com.streamsets.pipeline.stage.util.scripting.ScriptStageUtil;
import org.slf4j.Logger;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.List;
import java.util.Map;

public abstract class AbstractScriptingSource extends BasePushSource {
  private Logger log;
  private String scriptingEngineName;
  private String scriptConfigGroup;

  private final String script;
  private final Map<String, String> params;
  private final int numThreads;
  private final int batchSize;

  private CompiledScript compiledScript;
  private ScriptObjectFactory scriptObjectFactory;

  protected ScriptEngine engine;

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

    return issues;
  }


  protected SimpleBindings createBindings(Map<String, String> lastOffsets, int maxBatchSize) {
    SimpleBindings bindings = new SimpleBindings();
    ScriptingOriginBindings scriptingOriginBindings = new ScriptingOriginBindings(
        getScriptObjectFactory(), getContext(), params, log, numThreads,
        Math.min(batchSize, maxBatchSize), lastOffsets
    );
    bindings.put("sdc", scriptingOriginBindings);
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
    ScriptStageUtil.closeEngine(engine, getInfo(), log);
    engine = null;
  }

}
