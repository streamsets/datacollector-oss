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

package com.streamsets.pipeline.stage.destination.pulsar;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.lib.pulsar.config.PulsarGroups;

@StageDef(
    version = 2,
    label = "Pulsar Producer",
    description = "Write data to Pulsar topics",
    icon = "pulsar.png",
    recordsByRef = true,
    upgrader = PulsarTargetUpgrader.class,
    upgraderDef = "upgrader/PulsarDTarget.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_j5s_lpc_r2b",
    services = @ServiceDependency(
        service = DataFormatGeneratorService.class,
        configuration = {
            @ServiceConfiguration(name = "displayFormats", value = "BINARY,DATAGRAM,DELIMITED,JSON,LOG,PROTOBUF," +
                "SDC_JSON,TEXT,XML")
        }
    )
)
@ConfigGroups(PulsarGroups.class)
@GenerateResourceBundle
public class PulsarDTarget extends DTarget {

  @ConfigDefBean
  public PulsarTargetConfig pulsarConfig;

  @Override
  protected Target createTarget() {
    return new PulsarTarget(
        pulsarConfig,
        new PulsarMessageProducerFactoryImpl());
  }

}
