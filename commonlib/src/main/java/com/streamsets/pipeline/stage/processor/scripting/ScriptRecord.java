/*
 * Copyright 2017 StreamSets Inc.
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

import com.streamsets.pipeline.api.Record;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ScriptRecord {
  final Record record;
  public Object value;
  public final String stageCreator;
  public final String sourceId;
  public final String previousTrackingId;
  public final Map<String, String> attributes;
  public final String errorDataCollectorId;
  public final String errorPipelineName;
  public final String errorCode;
  public final String errorMessage;
  public final String errorStage;
  public final long errorTimestamp;
  public final String errorStackTrace;

  ScriptRecord(Record record, Object scriptObject) {
    this.record = record;
    this.stageCreator = record.getHeader().getStageCreator();
    this.sourceId = record.getHeader().getSourceId();
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
    this.errorTimestamp = record.getHeader().getErrorTimestamp();
    this.errorStackTrace = record.getHeader().getErrorStackTrace();

    value = scriptObject;
  }

}
