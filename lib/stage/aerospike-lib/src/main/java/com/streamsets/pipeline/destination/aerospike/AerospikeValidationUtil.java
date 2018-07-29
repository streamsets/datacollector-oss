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
package com.streamsets.pipeline.destination.aerospike;

import com.aerospike.client.Host;
import com.google.common.net.HostAndPort;
import com.streamsets.pipeline.api.Stage;

import java.util.ArrayList;
import java.util.List;

public class AerospikeValidationUtil {
  public List<Host> validateConnectionString(
      List<Stage.ConfigIssue> issues,
      String connectionString,
      String configGroupName,
      String configName,
      Stage.Context context
  ) {
    List<Host> clusterNodesList = new ArrayList<>();
    if (connectionString == null || connectionString.isEmpty()) {
      issues.add(context.createConfigIssue(configGroupName, configName,
          AerospikeErrors.AEROSPIKE_01, configName));
    } else {
      String[] nodes = connectionString.split(",");
      for (String node : nodes) {
        try {
          HostAndPort hostAndPort = HostAndPort.fromString(node);
          if(!hostAndPort.hasPort() || hostAndPort.getPort() < 0) {
            issues.add(context.createConfigIssue(configGroupName, configName, AerospikeErrors.AEROSPIKE_02, connectionString));
          } else {
            clusterNodesList.add(new Host(hostAndPort.getHostText(), hostAndPort.getPort()));
          }
        } catch (IllegalArgumentException e) {
          issues.add(context.createConfigIssue(configGroupName, configName, AerospikeErrors.AEROSPIKE_02, connectionString));
        }
      }
    }
    return clusterNodesList;
  }
}
