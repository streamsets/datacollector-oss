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
package com.streamsets.datacollector.event.dto;

import java.util.List;

public class SDCInfoEvent implements Event {

  private String sdcId;
  private String httpUrl;
  private String javaVersion;
  private List<StageInfo> stageInfoList;
  private SDCBuildInfo sdcBuildInfo;
  private List<String> labels;
  private boolean edge = false;
  private int offsetProtocolVersion;
  private String deploymentId;

  public SDCInfoEvent() {
  }

  public SDCInfoEvent(
      String id,
      String httpUrl,
      String javaVersion,
      List<StageInfo> stageInfoList,
      SDCBuildInfo sdcBuildInfo,
      List<String> labels,
      int offsetProtocolVersion,
      String deploymentId
  ) {
    this.sdcId = id;
    this.httpUrl = httpUrl;
    this.javaVersion = javaVersion;
    this.stageInfoList = stageInfoList;
    this.sdcBuildInfo = sdcBuildInfo;
    this.labels = labels;
    this.offsetProtocolVersion = offsetProtocolVersion;
    this.deploymentId = deploymentId;
  }

  public String getSdcId() {
    return sdcId;
  }

  public void setSdcId(String id) {
    this.sdcId = id;
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

  public List<StageInfo> getStageDefinitionList() {
    return stageInfoList;
  }

  public void setStageDefinitionList(List<StageInfo> stageDefinitionList) {
    this.stageInfoList = stageDefinitionList;
  }

  public SDCBuildInfo getSdcBuildInfo() {
    return sdcBuildInfo;
  }

  public void setSdcBuildInfo(SDCBuildInfo sdcBuildInfo) {
    this.sdcBuildInfo = sdcBuildInfo;
  }

  public List<StageInfo> getStageInfoList() {
    return stageInfoList;
  }

  public void setStageInfoList(List<StageInfo> stageInfoList) {
    this.stageInfoList = stageInfoList;
  }

  public List<String> getLabels() {
    return labels;
  }

  public void setLabels(List<String> labels) {
    this.labels = labels;
  }

  public boolean isEdge() {
    return edge;
  }

  public void setEdge(boolean edge) {
    this.edge = edge;
  }

  public int getOffsetProtocolVersion() {
    return offsetProtocolVersion;
  }

  public void setOffsetProtocolVersion(int offsetProtocolVersion) {
    this.offsetProtocolVersion = offsetProtocolVersion;
  }

  public String getDeploymentId() {
    return deploymentId;
  }

  public void setDeploymentId(String deploymentId) {
    this.deploymentId = deploymentId;
  }
}
