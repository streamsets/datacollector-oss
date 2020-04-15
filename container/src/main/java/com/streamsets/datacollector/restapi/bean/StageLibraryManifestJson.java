/*
 * Copyright 2018 StreamSets Inc.
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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StageLibraryManifestJson {

  private String manifestType;
  private String manifestVersion;
  private String signatureCrt;
  private String stageLibId;
  private String stageLibFile;
  private String stageLibFileSha1;
  private String stageLibFileSha256;
  private String stageLibLabel;
  private String stageLibVersion;
  private String stageLibLicense;
  private String stageLibMinSdcVersion;
  private List<StageInfoJson> stages;
  private boolean installed;

  @JsonCreator
  public StageLibraryManifestJson(
      @JsonProperty("manifest.type") String manifestType,
      @JsonProperty("manifest.version") String manifestVersion,
      @JsonProperty("signatureCrt") String signatureCrt,
      @JsonProperty("stagelib.id") String stageLibId,
      @JsonProperty("stagelib.file") String stageLibFile,
      @JsonProperty("stagelib.file.sha1") String stageLibFileSha1,
      @JsonProperty("stagelib.file.sha256") String stageLibFileSha256,
      @JsonProperty("stagelib.label") String stageLibLabel,
      @JsonProperty("stagelib.version") String stageLibVersion,
      @JsonProperty("stagelib.license") String stageLibLicense,
      @JsonProperty("stagelib.min.sdc.version") String stageLibMinSdcVersion,
      @JsonProperty("stage-libraries") List<StageInfoJson> stages
  ) {
    this.manifestType = manifestType;
    this.manifestVersion = manifestVersion;
    this.signatureCrt = signatureCrt;
    this.stageLibId = stageLibId;
    this.stageLibFile = stageLibFile;
    this.stageLibFileSha1 = stageLibFileSha1;
    this.stageLibFileSha256 = stageLibFileSha256;
    this.stageLibLabel = stageLibLabel;
    this.stageLibVersion = stageLibVersion;
    this.stageLibLicense = stageLibLicense;
    this.stageLibMinSdcVersion = stageLibMinSdcVersion;
    this.stages = stages;
  }

  public StageLibraryManifestJson(
      String stageLibId,
      String stageLibLabel,
      List<StageInfoJson> stages,
      boolean installed
  ) {

    this.stageLibId = stageLibId;
    this.stageLibLabel = stageLibLabel;
    this.stages = stages;
    this.installed = installed;
  }

  public String getManifestType() {
    return manifestType;
  }

  public void setManifestType(String manifestType) {
    this.manifestType = manifestType;
  }

  public String getManifestVersion() {
    return manifestVersion;
  }

  public void setManifestVersion(String manifestVersion) {
    this.manifestVersion = manifestVersion;
  }

  public String getSignatureCrt() {
    return signatureCrt;
  }

  public void setSignatureCrt(String signatureCrt) {
    this.signatureCrt = signatureCrt;
  }

  public String getStageLibId() {
    return stageLibId;
  }

  public void setStageLibId(String stageLibId) {
    this.stageLibId = stageLibId;
  }

  public String getStageLibFile() {
    return stageLibFile;
  }

  public void setStageLibFile(String stageLibFile) {
    this.stageLibFile = stageLibFile;
  }

  public String getStageLibFileSha1() {
    return stageLibFileSha1;
  }

  public void setStageLibFileSha1(String stageLibFileSha1) {
    this.stageLibFileSha1 = stageLibFileSha1;
  }

  public String getStageLibFileSha256() {
    return stageLibFileSha256;
  }

  public void setStageLibFileSha256(String stageLibFileSha256) {
    this.stageLibFileSha256 = stageLibFileSha256;
  }

  public String getStageLibLabel() {
    return stageLibLabel;
  }

  public void setStageLibLabel(String stageLibLabel) {
    this.stageLibLabel = stageLibLabel;
  }

  public String getStageLibVersion() {
    return stageLibVersion;
  }

  public void setStageLibVersion(String stageLibVersion) {
    this.stageLibVersion = stageLibVersion;
  }

  public String getStageLibLicense() {
    return stageLibLicense;
  }

  public void setStageLibLicense(String stageLibLicense) {
    this.stageLibLicense = stageLibLicense;
  }

  /**
   * @return the minimum supported SDC version for the stage library or empty if not specified.
   */
  public String getStageLibMinSdcVersion() {
    if (stageLibMinSdcVersion == null
        || stageLibMinSdcVersion.equalsIgnoreCase("undefined")
        || stageLibMinSdcVersion.equalsIgnoreCase("not-specified")) {
      return "";
    } else {
      return stageLibMinSdcVersion;
    }
  }

  public void setStageLibMinSdcVersion(String stageLibMinSdcVersion) {
    this.stageLibMinSdcVersion = stageLibMinSdcVersion == null ? "" : stageLibMinSdcVersion.trim();
  }

  public List<StageInfoJson> getStages() {
    return stages;
  }

  public void setStages(List<StageInfoJson> stages) {
    this.stages = stages;
  }

  public boolean isInstalled() {
    return installed;
  }

  public void setInstalled(boolean installed) {
    this.installed = installed;
  }
}
