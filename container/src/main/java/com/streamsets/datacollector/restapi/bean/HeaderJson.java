/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
  public HeaderJson(@JsonProperty("stageCreator") String stageCreator,
                    @JsonProperty("sourceId") String sourceId,
                    @JsonProperty("stagesPath") String stagesPath,
                    @JsonProperty("trackingId") String trackingId,
                    @JsonProperty("previousTrackingId") String previousTrackingId,
                    @JsonProperty("raw") byte[] raw,
                    @JsonProperty("rawMimeType") String rawMimeType,
                    @JsonProperty("errorDataCollectorId") String errorDataCollectorId,
                    @JsonProperty("errorPipelineName") String errorPipelineName,
                    @JsonProperty("errorStage") String errorStageInstance,
                    @JsonProperty("errorCode") String errorCode,
                    @JsonProperty("errorMessage") String errorMessage,
                    @JsonProperty("errorTimestamp") long errorTimestamp,
                    @JsonProperty("values") Map<String, Object> map) {
    this.header = new HeaderImpl(stageCreator, sourceId, stagesPath, trackingId, previousTrackingId, raw, rawMimeType,
      errorDataCollectorId, errorPipelineName, errorStageInstance, errorCode, errorMessage, errorTimestamp, map);
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

  public long getErrorTimestamp() {return header.getErrorTimestamp();}

  public Map<String, String> getValues() {return header.getValues();}

  @JsonIgnore
  public HeaderImpl getHeader() {
    return header;
  }
}
