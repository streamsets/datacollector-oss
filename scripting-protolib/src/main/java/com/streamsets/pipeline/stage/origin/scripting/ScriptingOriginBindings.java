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
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.util.scripting.ScriptObjectFactory;
import com.streamsets.pipeline.stage.util.scripting.ScriptRecord;
import com.streamsets.pipeline.stage.util.scripting.ScriptingStageBindings;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ScriptingOriginBindings extends ScriptingStageBindings {

  private final PushSource.Context context;

  public final int numThreads;
  public final int batchSize;
  public final Map<String, String> lastOffsets;

  public class PushSourceScriptBatch {
    private int batchSize;
    private BatchContext batchContext;
    private String[] allLanes;

    PushSourceScriptBatch () {
      batchContext = context.startBatch();
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
      return context.processBatch(batchContext, entityName, entityOffset);
    }

    public void add(ScriptRecord scriptRecord) {
      batchSize++;
      Record record = scriptObjectFactory.getRecord(scriptRecord);
      batchContext.getBatchMaker().addRecord(record, allLanes);
    }

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
        scriptRecords.add(scriptObjectFactory.createScriptRecord(record));
      }
      return scriptRecords;
    }
  }


  public ScriptingOriginBindings(
      ScriptObjectFactory scriptObjectFactory,
      PushSource.Context context,
      ErrorRecordHandler errorRecordHandler,
      Map<String, String> userParams,
      Logger log,
      int numThreads,
      int batchSize,
      Map<String, String> lastOffsets
      ) {
    super(scriptObjectFactory, context, errorRecordHandler, userParams, log);
    this.context = context;
    this.numThreads = numThreads;
    this.batchSize = batchSize;
    this.lastOffsets = lastOffsets;
  }


  public PushSourceScriptBatch createBatch() {
    return new PushSourceScriptBatch();
  }

}
