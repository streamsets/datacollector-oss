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

import com.streamsets.datacollector.config.ConnectionDefinition;
import com.streamsets.datacollector.definition.ConnectionVerifierDefinition;

import java.util.List;

public class ConnectionsJson {

  private final String schemaVersion = "1";
  private List<ConnectionDefinition> connections;
  private ConnectionVerifierDefinition verifierDefinition;

  public ConnectionsJson() {}

  public ConnectionsJson(List<ConnectionDefinition> connections, ConnectionVerifierDefinition verifierDefinition) {
    this.connections = connections;
    this.verifierDefinition = verifierDefinition;
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public List<ConnectionDefinition> getConnections() {
    return connections;
  }

  public void setConnections(List<ConnectionDefinition> connections) {
    this.connections = connections;
  }
}
