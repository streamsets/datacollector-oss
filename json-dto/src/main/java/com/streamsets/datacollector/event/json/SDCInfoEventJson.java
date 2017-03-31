/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.event.json;

import java.util.List;

public class SDCInfoEventJson implements EventJson {

  private String sdcId;
  private String httpUrl;
  private String javaVersion;
  private List<StageInfoJson> stageInfoList;
  private SDCBuildInfoJson sdcBuildInfo;
  private List<String> labels;
  // default for sdc 2.4 and below is 1
  private int offsetProtocolVersion = 1;

  public String getSdcId() {
    return sdcId;
  }

  public void setSdcId(String sdcId) {
    this.sdcId = sdcId;
  }

  public String getHttpUrl() {
    return httpUrl;
  }

  public void setHttpUrl(String httpUrl) {
    this.httpUrl = httpUrl;
  }

  public String getJavaVersion() {
    return javaVersion;
  }

  public void setJavaVersion(String javaVersion) {
    this.javaVersion = javaVersion;
  }

  public List<StageInfoJson> getStageDefinitionList() {
    return stageInfoList;
  }

  public void setStageDefinitionList(List<StageInfoJson> stageDefinitionList) {
    this.stageInfoList = stageDefinitionList;
  }

  public SDCBuildInfoJson getSdcBuildInfo() {
    return sdcBuildInfo;
  }

  public void setSdcBuildInfo(SDCBuildInfoJson sdcBuildInfo) {
    this.sdcBuildInfo = sdcBuildInfo;
  }

  public List<StageInfoJson> getStageInfoList() {
    return stageInfoList;
  }

  public void setStageInfoList(List<StageInfoJson> stageInfoList) {
    this.stageInfoList = stageInfoList;
  }

  public List<String> getLabels() {
    return labels;
  }

  public void setLabels(List<String> labels) {
    this.labels = labels;
  }

  public int getOffsetProtocolVersion() {
    return offsetProtocolVersion;
  }

  public void setOffsetProtocolVersion(int offsetProtocolVersion) {
    this.offsetProtocolVersion = offsetProtocolVersion;
  }
}
