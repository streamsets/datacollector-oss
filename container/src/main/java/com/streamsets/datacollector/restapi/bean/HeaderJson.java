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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.record.HeaderImpl;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Map;

public class HeaderJson {

  private final HeaderImpl header;

  @JsonCreator
  public HeaderJson(
      @JsonProperty("stageCreator") String stageCreator,
      @JsonProperty("sourceId") String sourceId,
      @JsonProperty("stagesPath") String stagesPath,
      @JsonProperty("trackingId") String trackingId,
      @JsonProperty("previousTrackingId") String previousTrackingId,
      @JsonProperty("raw") byte[] raw,
      @JsonProperty("rawMimeType") String rawMimeType,
      @JsonProperty("errorDataCollectorId") String errorDataCollectorId,
      @JsonProperty("errorPipelineName") String errorPipelineName,
      @JsonProperty("errorStage") String errorStage,
      @JsonProperty("errorStageLabel") String errorStageLabel,
      @JsonProperty("errorCode") String errorCode,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("errorTimestamp") long errorTimestamp,
      @JsonProperty("errorStackTrace") String errorStackTrace,
      @JsonProperty("errorJobId") String errorJobId,
      @JsonProperty("errorJobName") String errorJobName,
      @JsonProperty("values") Map<String, Object> map
  ) {
    this.header = new HeaderImpl(
      stageCreator,
      sourceId,
      stagesPath,
      trackingId,
      previousTrackingId,
      raw,
      rawMimeType,
      errorDataCollectorId,
      errorPipelineName,
      errorStage,
      errorStageLabel,
      errorCode,
      errorMessage,
      errorTimestamp,
      errorStackTrace,
      map,
      errorJobId,
      errorJobName
    );
  }

  public HeaderJson(HeaderImpl header) {
    Utils.checkNotNull(header, "header");
    this.header = header;
  }

  public String getStageCreator() {return header.getStageCreator();}

  public String getSourceId() {return header.getSourceId();}

  public String getTrackingId() {return header.getTrackingId();}

  public String getPreviousTrackingId() {return header.getPreviousTrackingId();}

  public String getStagesPath() {return header.getStagesPath();}

  public byte[] getRaw() {return header.getRaw();}

  public String getRawMimeType() {return header.getRawMimeType();}

  public String getErrorDataCollectorId() {return header.getErrorDataCollectorId();}

  public String getErrorPipelineName() {return header.getErrorPipelineName();}

  public String getErrorCode() {return header.getErrorCode();}

  public String getErrorMessage() {return header.getErrorMessage();}

  public String getErrorStage() {return header.getErrorStage();}

  public String getErrorJobId() {return header.getErrorJobId();}

  public String getErrorJobName() {return header.getErrorJobName();}

  public String getErrorStageLabel() {return header.getErrorStageLabel();}

  public long getErrorTimestamp() {return header.getErrorTimestamp();}

  public String getErrorStackTrace() {return header.getErrorStackTrace();}

  public Map<String, String> getValues() {return header.getValues();}

  @JsonIgnore
  public HeaderImpl getHeader() {
    return header;
  }
}
