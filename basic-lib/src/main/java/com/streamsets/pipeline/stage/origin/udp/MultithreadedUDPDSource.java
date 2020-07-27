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
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.api.impl.Utils;

@StageDef(
    version = 1,
    label = "UDP Multithreaded Source",
    description = "Listens for UDP messages on one or more port(s) and queues incoming packets on an intermediate" +
        " queue, from which multiple worker threads can process them",
    icon = "udp.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    upgraderDef = "upgrader/MultithreadedUDPDSource.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_g2k_v5f_5bb"
)

@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class MultithreadedUDPDSource extends DPushSource {
  @ConfigDefBean
  public UDPSourceConfigBean configs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Packet Queue Size",
      description = "Maximum number of datagram packets that will be kept in the intermediate queue.",
      defaultValue = "200000",
      group = "UDP",
      min = 1,
      max = Integer.MAX_VALUE,
      displayPosition = 200,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public int packetQueueSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Number of Worker Threads",
      description = "Number of worker threads processing packets from the intermediate queue and running the pipeline.",
      defaultValue = "1",
      group = "UDP",
      min = 1,
      displayPosition = 210,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public int numWorkerThreads;

  @Override
  protected PushSource createPushSource() {
    Utils.checkNotNull(configs.dataFormat, "Data format cannot be null");
    Utils.checkNotNull(configs.ports, "Ports cannot be null");

    return new MultithreadedUDPSource(
        configs,
        packetQueueSize,
        numWorkerThreads
    );
  }
}
