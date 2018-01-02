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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.google.common.base.Joiner;
import com.google.common.net.HostAndPort;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Target;

import java.util.ArrayList;
import java.util.List;

public class AerospikeBeanConfig {


  @ConfigDef(
      required = false,
      type = ConfigDef.Type.LIST,
      label = "Aerospike nodes",
      description = "List of Aerospike nodes. Use format <HOST>:<PORT>",
      displayPosition = 10,
      group = "AEROSPIKE"
  )
  public List<String> connectionString;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Retry Attempts",
      description = "0 means not retrying, needs to be 0 or more",
      defaultValue = "0",
      required = true,
      min = 0,
      displayPosition = 20,
      group = "AEROSPIKE"
  )
  public int maxRetries = 0;


  private AerospikeClient client;

  /**
   * initialize and validate configuration options
   *
   * @param context
   * @param issues
   */
  public void init(Target.Context context, List<Target.ConfigIssue> issues) {
    List<Host> hosts = getAerospikeHosts(issues, connectionString, Groups.AEROSPIKE.getLabel(), "aerospikeBeanConfig.connectionString", context);
    ClientPolicy cp = new ClientPolicy();
    try {
      client = new AerospikeClient(cp, hosts.toArray(new Host[hosts.size()]));
      int retries = 0;
      while (!client.isConnected() && retries <= maxRetries) {
        if (retries > maxRetries) {
          issues.add(context.createConfigIssue(Groups.AEROSPIKE.getLabel(), "aerospikeBeanConfig.connectionString", AerospikeErrors.AEROSPIKE_03, connectionString));
          return;
        }
        retries++;
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
      }

    } catch (AerospikeException ex) {
      issues.add(context.createConfigIssue(Groups.AEROSPIKE.getLabel(), "aerospikeBeanConfig.connectionString", AerospikeErrors.AEROSPIKE_03, connectionString));
    }
  }

  public static List<Host> getAerospikeHosts(
      List<Target.ConfigIssue> issues,
      List<String> connectionString,
      String configGroupName,
      String configName,
      Target.Context context
  ) {
    List<Host> clusterNodesList = new ArrayList<>();
    if (connectionString == null || connectionString.isEmpty()) {
      issues.add(context.createConfigIssue(configGroupName, configName,
          AerospikeErrors.AEROSPIKE_01, configName));
    } else {
      for (String node : connectionString) {
        try {
          HostAndPort hostAndPort = HostAndPort.fromString(node);
          if (!hostAndPort.hasPort() || hostAndPort.getPort() < 0) {
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

  public AerospikeClient getAerospikeClient() {
    return this.client;
  }

  public void destroy() {
    if (client != null) {
      client.close();
    }
  }
}
