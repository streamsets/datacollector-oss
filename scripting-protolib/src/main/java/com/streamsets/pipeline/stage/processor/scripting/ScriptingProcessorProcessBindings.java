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
package com.streamsets.pipeline.stage.processor.scripting;

import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.util.scripting.ScriptObjectFactory;
import com.streamsets.pipeline.stage.util.scripting.ScriptRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

public class ScriptingProcessorProcessBindings extends ScriptingProcessorInitDestroyBindings {

  public final ScriptingProcessorOutput output;
  public final Object[] records;

  public ScriptingProcessorProcessBindings(
      ScriptObjectFactory scriptObjectFactory,
      Processor.Context context,
      ErrorRecordHandler errorRecordHandler,
      Map<String, String> userParams,
      Logger log,
      Object state,
      ScriptingProcessorOutput output,
      @NotNull List<ScriptRecord> records
      ) {
    super(scriptObjectFactory, context, errorRecordHandler, userParams, log, state);
    this.output = output;
    this.records = records.toArray(new Object[0]);
  }

}
