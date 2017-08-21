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
package com.streamsets.pipeline.stage.origin.udptokafka;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.origin.lib.UDPDataFormat;
import com.streamsets.pipeline.stage.origin.lib.UDPDataFormatChooserValues;
import io.netty.channel.epoll.Epoll;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class UDPConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Port",
      defaultValue = "[\"9995\"]",
      description = "Port to listen on",
      group = "UDP",
      displayPosition = 10)
  public List<String> ports; // string so we can listen on multiple ports in the future

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      defaultValue = "SYSLOG",
      group = "UDP",
      displayPosition = 20)
  @ValueChooserModel(UDPDataFormatChooserValues.class)
  public UDPDataFormat dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Enable UDP Multithreading",
      description = "Use multiple receiver threads for each port. Only available on 64-bit Linux systems",
      defaultValue = "false",
      group = "ADVANCED",
      displayPosition = 25
  )
  public boolean enableEpoll;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Accept Threads",
      description = "Number of receiver threads for each port. It should be based on the CPU cores expected to be dedicated to the pipeline",
      defaultValue = "1",
      min = 1,
      max = 32,
      group = "ADVANCED",
      dependsOn = "enableEpoll",
      triggeredByValue = "true",
      displayPosition = 26)
  public int acceptThreads;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Write Concurrency",
      description = "it determines the number of Kafka clients to use to write to Kafka",
      defaultValue = "50",
      min = 1,
      max = 2048,
      group = "ADVANCED",
      displayPosition = 30)
  public int concurrency;

  private List<InetSocketAddress> addresses;
  private boolean privilegedPortUsage = false;

  public List<Stage.ConfigIssue> init(Stage.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    addresses = new ArrayList<>();
    for (String candidatePort : ports) {
      try {
        int port = Integer.parseInt(candidatePort.trim());
        if (port > 0 && port < 65536) {
          if (port < 1024) {
            privilegedPortUsage = true; // only for error handling purposes
          }
          addresses.add(new InetSocketAddress(port));
        } else {
          issues.add(context.createConfigIssue(Groups.UDP.name(), "ports", Errors.UDP_KAFKA_ORIG_00, port));
        }
      } catch (NumberFormatException ex) {
        issues.add(context.createConfigIssue(Groups.UDP.name(),
            "ports",
            Errors.UDP_KAFKA_ORIG_00,
            candidatePort
        ));
      }
    }
    if (addresses.isEmpty()) {
      issues.add(context.createConfigIssue(Groups.UDP.name(), "ports", Errors.UDP_KAFKA_ORIG_03));
    }
    if (acceptThreads < 1 || acceptThreads > 32) {
      issues.add(context.createConfigIssue(Groups.ADVANCED.name(), "acceptThreads", Errors.UDP_KAFKA_ORIG_05));
    }
    if (concurrency < 1 || concurrency > 2048) {
      issues.add(context.createConfigIssue(Groups.UDP.name(), "concurrency", Errors.UDP_KAFKA_ORIG_04));
    }

    if (enableEpoll && !Epoll.isAvailable()) {
      issues.add(context.createConfigIssue(Groups.UDP.name(), "enableEpoll", Errors.UDP_KAFKA_ORIG_06));
    }

    // Force threads to 1 if epoll not enabled
    if (!enableEpoll) {
      acceptThreads = 1;
    }
    return issues;
  }

  public List<InetSocketAddress> getAddresses() {
    return addresses;
  }

  public boolean isPrivilegedPortUsage() {
    return privilegedPortUsage;
  }

  public void destroy() {
  }

}
