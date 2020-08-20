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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.datacollector.definition.ConnectionVerifierDefinition;

import java.util.List;

/**
 * Represents the connection information parameters received in a dynamic preview request for a connection verifier
 *
 */
public class ConnectionDefinitionPreviewJson {

  private String version;
  private String type;
  private List<ConfigConfigurationJson> configuration;
  private ConnectionVerifierDefinition verifierDefinition;

  @JsonIgnore
  public String connectionId;

  public ConnectionDefinitionPreviewJson() { }

  public ConnectionDefinitionPreviewJson(
      String version,
      String type,
      List<ConfigConfigurationJson> configuration,
      ConnectionVerifierDefinition verifierDefinition
  ) {
    this.version = version;
    this.type = type;
    this.configuration = configuration;
    this.verifierDefinition = verifierDefinition;
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

  public List<ConfigConfigurationJson> getConfiguration() {
    return configuration;
  }

  public void setConfiguration(List<ConfigConfigurationJson> configuration) {
    this.configuration = configuration;
  }

  public String getConnectionId() {
    return connectionId;
  }

  public void setConnectionId(String connectionId) {
    this.connectionId = connectionId;
  }

  public ConnectionVerifierDefinition getVerifierDefinition() {
    return verifierDefinition;
  }

  public void setVerifierDefinition(ConnectionVerifierDefinition verifierDefinition) {
    this.verifierDefinition = verifierDefinition;
  }

  public String getVerifierStageName() {
    return verifierDefinition.getVerifierClass().replace(".", "_");
  }
}
