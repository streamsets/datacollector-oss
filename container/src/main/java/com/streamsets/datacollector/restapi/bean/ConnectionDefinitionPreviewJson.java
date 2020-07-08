/*
 * Copyright 2020 StreamSets Inc.
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

import java.util.List;

/**
 * Represents the connection information parameters received in a dynamic preview request for a connection verifier
 *
 */
public class ConnectionDefinitionPreviewJson {

  private String version;
  private String type;
  private List<ConfigConfigurationJson> configuration;
  private String verifierClass;
  private String verifierPrefix;
  private String library;

  public ConnectionDefinitionPreviewJson() { }

  public ConnectionDefinitionPreviewJson(
      String version,
      String type,
      List<ConfigConfigurationJson> configuration,
      String verifierClass,
      String verifierPrefix,
      String library
  ) {
    this.version = version;
    this.type = type;
    this.configuration = configuration;
    this.verifierClass = verifierClass;
    this.verifierPrefix = verifierPrefix;
    this.library = library;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getVerifierClass() {
    return verifierClass;
  }

  public void setVerifierClass(String verifierClass) {
    this.verifierClass = verifierClass;
  }

  public List<ConfigConfigurationJson> getConfiguration() {
    return configuration;
  }

  public void setConfiguration(List<ConfigConfigurationJson> configuration) {
    this.configuration = configuration;
  }

  public String getLibrary() {
    return library;
  }

  public void setLibrary(String library) {
    this.library = library;
  }

  public String getVerifierStageName() {
    return verifierClass.replace(".", "_");
  }

  public String getVerifierPrefix() {
    return verifierPrefix;
  }

  public void setVerifierPrefix(String verifierPrefix) {
    this.verifierPrefix = verifierPrefix;
  }
}
