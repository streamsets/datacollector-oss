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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToEventContext;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ScriptingOriginBindings {
  private final ScriptObjectFactory scriptObjectFactory;
  private final PushSource.Context context;
  private final ErrorRecordHandler errorRecordHandler;

  // Needed to address SDC-8697 where simultaneously started Jython pipelines cause import errors
  private static Lock lock = new ReentrantLock();

  public final Err error;
  public final Map<String, String> userParams;
  public final Logger log;
  public final int numThreads;
  public final int batchSize;
  public final Map<String, String> lastOffsets;

  // These must be bound to the same objects instantiated in ScriptTypedNullObject.
  // They will be decoded later by "==" comparison to these objects.
  public static final Object NULL_BOOLEAN = ScriptTypedNullObject.NULL_BOOLEAN;
  public static final Object NULL_CHAR = ScriptTypedNullObject.NULL_CHAR; //Groovy support char
  public static final Object NULL_BYTE = ScriptTypedNullObject.NULL_BYTE; //Groovy support byte
  public static final Object NULL_SHORT = ScriptTypedNullObject.NULL_SHORT; //Groovy support short
  public static final Object NULL_INTEGER = ScriptTypedNullObject.NULL_INTEGER;
  public static final Object NULL_LONG = ScriptTypedNullObject.NULL_LONG;
  public static final Object NULL_FLOAT = ScriptTypedNullObject.NULL_FLOAT;
  public static final Object NULL_DOUBLE = ScriptTypedNullObject.NULL_DOUBLE;
  public static final Object NULL_DATE = ScriptTypedNullObject.NULL_DATE;
  public static final Object NULL_DATETIME = ScriptTypedNullObject.NULL_DATETIME;
  public static final Object NULL_TIME = ScriptTypedNullObject.NULL_TIME;
  public static final Object NULL_DECIMAL = ScriptTypedNullObject.NULL_DECIMAL;
  public static final Object NULL_BYTE_ARRAY = ScriptTypedNullObject.NULL_BYTE_ARRAY;
  public static final Object NULL_STRING = ScriptTypedNullObject.NULL_STRING;
  public static final Object NULL_LIST = ScriptTypedNullObject.NULL_LIST;
  public static final Object NULL_MAP = ScriptTypedNullObject.NULL_MAP;

  // to hide all other methods of Stage.Context
  public class Err {
    public void write(ScriptRecord scriptRecord, String errMsg) throws StageException {
      errorRecordHandler.onError(new OnRecordErrorException(
          scriptObjectFactory.getRecord(scriptRecord),
          Errors.SCRIPTING_04,
          errMsg
      ));
    }
  }

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
    this.scriptObjectFactory = scriptObjectFactory;
    this.context = context;
    this.errorRecordHandler = errorRecordHandler;
    this.userParams = userParams;
    this.log = log;
    this.numThreads = numThreads;
    this.batchSize = batchSize;
    this.lastOffsets = lastOffsets;
    this.error = new Err();
  }

  // To access getFieldNull function through SimpleBindings
  public Object getFieldNull(ScriptRecord scriptRecord, String fieldPath) {
    return ScriptTypedNullObject.getFieldNull(scriptObjectFactory.getRecord(scriptRecord), fieldPath);
  }

  /**
   * Create record
   * Note: Default field value is null.
   * @param recordSourceId the unique record id for this record.
   * @return ScriptRecord The Newly Created Record
   */
  public ScriptRecord createRecord(String recordSourceId) {
    return scriptObjectFactory.createScriptRecord(context.createRecord(recordSourceId));
  }

  public ScriptRecord createEvent(String type, int version) {
    String recordSourceId = Utils.format("event:{}:{}:{}", type, version, System.currentTimeMillis());
    EventRecord er = context.createEventRecord(type, version, recordSourceId);
    ScriptRecord sr = scriptObjectFactory.createScriptRecord(er);
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

    ((ToEventContext) context).toEvent((EventRecord)scriptObjectFactory.getRecord(event));
  }

  public boolean isPreview() { return context.isPreview(); }

  public Object createMap(boolean listMap) {
    return scriptObjectFactory.createMap(listMap);
  }

  public Map<String, Object> pipelineParameters() {
    return context.getPipelineConstants();
  }

  public PushSourceScriptBatch createBatch() {
    return new PushSourceScriptBatch();
  }

  public boolean isStopped() {
    return context.isStopped();
  }

  // Needed to address SDC-8697 where simultaneously started Jython pipelines cause import errors
  public void importLock() {
    lock.lock();
  }

  // Needed to address SDC-8697 where simultaneously started Jython pipelines cause import errors
  public void importUnlock() {
    lock.unlock();
  }

}
