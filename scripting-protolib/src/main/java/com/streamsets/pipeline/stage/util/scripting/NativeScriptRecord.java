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

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.stage.util.scripting.ScriptRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class NativeScriptRecord extends ScriptRecord {
  public final Record sdcRecord;
  public Object value;
  public final String stageCreator;
  public final String sourceId;
  public final String stagesPath;
  public final String trackingId;
  public final String previousTrackingId;
  public final Map<String, String> attributes;
  public final String errorDataCollectorId;
  public final String errorPipelineName;
  public final String errorCode;
  public final String errorMessage;
  public final String errorStage;
  public final String errorStageLabel;
  public final long errorTimestamp;
  public final String errorStackTrace;
  public final String errorJobId;

  NativeScriptRecord(Record record, Object scriptObject) {
    this.sdcRecord = record;
    this.stageCreator = record.getHeader().getStageCreator();
    this.sourceId = record.getHeader().getSourceId();
    this.stagesPath = record.getHeader().getStagesPath();
    this.trackingId = record.getHeader().getTrackingId();
    this.previousTrackingId = record.getHeader().getPreviousTrackingId();
    Set<String> headerAttributeNames = record.getHeader().getAttributeNames();
    attributes = new HashMap<>();
    for (String key : headerAttributeNames) {
      attributes.put(key, record.getHeader().getAttribute(key));
    }
    this.errorDataCollectorId = record.getHeader().getErrorDataCollectorId();
    this.errorPipelineName = record.getHeader().getErrorPipelineName();
    this.errorCode = record.getHeader().getErrorCode();
    this.errorMessage = record.getHeader().getErrorMessage();
    this.errorStage = record.getHeader().getErrorStage();
    this.errorStageLabel = record.getHeader().getErrorStageLabel();
    this.errorTimestamp = record.getHeader().getErrorTimestamp();
    this.errorStackTrace = record.getHeader().getErrorStackTrace();
    this.errorJobId = record.getHeader().getErrorJobId();

    this.value = scriptObject;
  }
}
