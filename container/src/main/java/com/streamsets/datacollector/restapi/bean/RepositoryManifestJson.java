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
public class RepositoryManifestJson {
  private String manifestType;
  private String manifestVersion;
  private String signatureCrt;
  private String repoLabel;
  private String stageLibrariesVersion;
  private String stageLibrariesLicense;
  private String stageLibrariesMinSdcVersion;
  private List<StageLibrariesJson> stageLibraries;
  private String repoUrl;

  public RepositoryManifestJson() {

  }

  @JsonCreator
  public RepositoryManifestJson(
      @JsonProperty("manifest.type") String manifestType,
      @JsonProperty("manifest.version") String manifestVersion,
      @JsonProperty("signatureCrt") String signatureCrt,
      @JsonProperty("repo.label") String repoLabel,
      @JsonProperty("stage-libraries.version") String stageLibrariesVersion,
      @JsonProperty("stage-libraries.license") String stageLibrariesLicense,
      @JsonProperty("stage-libraries.min.sdc.version") String stageLibrariesMinSdcVersion,
      @JsonProperty("stage-libraries") List<StageLibrariesJson> stageLibraries
  ) {
    this.manifestType = manifestType;
    this.manifestVersion = manifestVersion;
    this.signatureCrt = signatureCrt;
    this.repoLabel = repoLabel;
    this.stageLibrariesVersion = stageLibrariesVersion;
    this.stageLibrariesLicense = stageLibrariesLicense;
    this.stageLibrariesMinSdcVersion = stageLibrariesMinSdcVersion;
    this.stageLibraries = stageLibraries;
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

  public String getRepoLabel() {
    return repoLabel;
  }

  public void setRepoLabel(String repoLabel) {
    this.repoLabel = repoLabel;
  }

  public String getStageLibrariesVersion() {
    return stageLibrariesVersion;
  }

  public void setStageLibrariesVersion(String stageLibrariesVersion) {
    this.stageLibrariesVersion = stageLibrariesVersion;
  }

  public String getStageLibrariesLicense() {
    return stageLibrariesLicense;
  }

  public void setStageLibrariesLicense(String stageLibrariesLicense) {
    this.stageLibrariesLicense = stageLibrariesLicense;
  }

  /**
   * @return the minimum supported SDC version for the stage libraries or empty if not specified.
   */
  public String getStageLibrariesMinSdcVersion() {
    if (stageLibrariesMinSdcVersion == null
        || stageLibrariesMinSdcVersion.equalsIgnoreCase("undefined")
        || stageLibrariesMinSdcVersion.equalsIgnoreCase("not-specified")) {
      return "";
    } else {
      return stageLibrariesMinSdcVersion;
    }
  }

  public void setStageLibrariesMinSdcVersion(String stageLibrariesMinSdcVersion) {
    this.stageLibrariesMinSdcVersion = stageLibrariesMinSdcVersion == null ? "" : stageLibrariesMinSdcVersion.trim();
  }

  public List<StageLibrariesJson> getStageLibraries() {
    return stageLibraries;
  }

  public void setStageLibraries(List<StageLibrariesJson> stageLibraries) {
    this.stageLibraries = stageLibraries;
  }

  public String getRepoUrl() {
    return repoUrl;
  }

  public void setRepoUrl(String repoUrl) {
    this.repoUrl = repoUrl;
  }
}
