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
package com.streamsets.pipeline.stage.util.scripting;

import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToEventContext;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class ScriptingStageBindings {

  protected final ScriptObjectFactory scriptObjectFactory;
  protected final Stage.Context context;

  // Needed to address SDC-8697 where simultaneously started Jython pipelines cause import errors
  private static Lock lock = new ReentrantLock();

  public final Map<String, String> userParams;
  public final Logger log;

  // These must be bound to the same objects instantiated in ScriptTypedNullObject.
  // They will be decoded later by "==" comparison to these objects.
  public final Object NULL_BOOLEAN = ScriptTypedNullObject.NULL_BOOLEAN;
  public final Object NULL_CHAR = ScriptTypedNullObject.NULL_CHAR; //Groovy support char
  public final Object NULL_BYTE = ScriptTypedNullObject.NULL_BYTE; //Groovy support byte
  public final Object NULL_SHORT = ScriptTypedNullObject.NULL_SHORT; //Groovy support short
  public final Object NULL_INTEGER = ScriptTypedNullObject.NULL_INTEGER;
  public final Object NULL_LONG = ScriptTypedNullObject.NULL_LONG;
  public final Object NULL_FLOAT = ScriptTypedNullObject.NULL_FLOAT;
  public final Object NULL_DOUBLE = ScriptTypedNullObject.NULL_DOUBLE;
  public final Object NULL_DATE = ScriptTypedNullObject.NULL_DATE;
  public final Object NULL_DATETIME = ScriptTypedNullObject.NULL_DATETIME;
  public final Object NULL_TIME = ScriptTypedNullObject.NULL_TIME;
  public final Object NULL_DECIMAL = ScriptTypedNullObject.NULL_DECIMAL;
  public final Object NULL_BYTE_ARRAY = ScriptTypedNullObject.NULL_BYTE_ARRAY;
  public final Object NULL_STRING = ScriptTypedNullObject.NULL_STRING;
  public final Object NULL_LIST = ScriptTypedNullObject.NULL_LIST;
  public final Object NULL_MAP = ScriptTypedNullObject.NULL_MAP;

  public ScriptingStageBindings(
      ScriptObjectFactory scriptObjectFactory,
      Stage.Context context,
      Map<String, String> userParams,
      Logger log
      ) {
    this.scriptObjectFactory = scriptObjectFactory;
    this.context = context;
    this.userParams = userParams;
    this.log = log;
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
    EventRecord eventRecord = context.createEventRecord(type, version, recordSourceId);
    ScriptRecord scriptRecord = scriptObjectFactory.createScriptRecord(eventRecord);
    return scriptRecord;
  }


  public boolean isPreview() { return context.isPreview(); }

  public boolean isStopped() {
    return context.isStopped();
  }

  public Map<String, Object> pipelineParameters() {
    return context.getPipelineConstants();
  }

  public Object createMap(boolean listMap) {
    return scriptObjectFactory.createMap(listMap);
  }

  // To access getFieldNull function through SimpleBindings
  public Object getFieldNull(ScriptRecord scriptRecord, String fieldPath) {
    return ScriptTypedNullObject.getFieldNull(scriptObjectFactory.getRecord(scriptRecord), fieldPath);
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
