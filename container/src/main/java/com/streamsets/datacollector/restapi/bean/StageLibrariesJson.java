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

@JsonIgnoreProperties(ignoreUnknown = true)
public class StageLibrariesJson {
  private String stagelibManifest;
  private String stagelibVersion;
  private String stagelibManifestSha1;
  private String stagelibManifestSha256;
  private boolean legacy;
  private StageLibraryManifestJson stageLibraryManifest;

  public StageLibrariesJson() {

  }

  @JsonCreator
  public StageLibrariesJson(
      @JsonProperty("stagelib.manifest") String stagelibManifest,
      @JsonProperty("stagelib.version") String stagelibVersion,
      @JsonProperty("stagelib.manifest.sha1") String stagelibManifestSha1,
      @JsonProperty("stagelib.manifest.sha256") String stagelibManifestSha256
  ) {
    this.stagelibManifest = stagelibManifest;
    this.stagelibVersion = stagelibVersion;
    this.stagelibManifestSha1 = stagelibManifestSha1;
    this.stagelibManifestSha256 = stagelibManifestSha256;
  }

  public String getStagelibManifest() {
    return stagelibManifest;
  }

  public void setStagelibManifest(String stagelibManifest) {
    this.stagelibManifest = stagelibManifest;
  }

  public String getStagelibVersion() {
    return stagelibVersion;
  }

  public void setStagelibVersion(String stagelibVersion) {
    this.stagelibVersion = stagelibVersion;
  }

  public String getStagelibManifestSha1() {
    return stagelibManifestSha1;
  }

  public void setStagelibManifestSha1(String stagelibManifestSha1) {
    this.stagelibManifestSha1 = stagelibManifestSha1;
  }

  public String getStagelibManifestSha256() {
    return stagelibManifestSha256;
  }

  public void setStagelibManifestSha256(String stagelibManifestSha256) {
    this.stagelibManifestSha256 = stagelibManifestSha256;
  }

  public boolean isLegacy() {
    return legacy;
  }

  public void setLegacy(boolean legacy) {
    this.legacy = legacy;
  }

  public StageLibraryManifestJson getStageLibraryManifest() {
    return stageLibraryManifest;
  }

  public void setStageLibraryManifest(StageLibraryManifestJson stageLibraryManifest) {
    this.stageLibraryManifest = stageLibraryManifest;
  }
}
