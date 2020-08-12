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

public class ConnectionsJson {

  private final String schemaVersion = "1";
  private List<ConnectionDefinitionJson> connections;

  public ConnectionsJson() {}

  public ConnectionsJson(List<ConnectionDefinitionJson> connections) {
    this.connections = connections;
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public List<ConnectionDefinitionJson> getConnections() {
    return connections;
  }

  public void setConnections(List<ConnectionDefinitionJson> connections) {
    this.connections = connections;
  }
}
