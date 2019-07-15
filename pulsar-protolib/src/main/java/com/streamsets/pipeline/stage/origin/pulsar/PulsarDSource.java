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

package com.streamsets.pipeline.stage.origin.pulsar;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSourceOffsetCommitter;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.lib.pulsar.config.PulsarGroups;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.MessageConfig;

@StageDef(
    version = 1,
    label = "Pulsar Consumer",
    description = "Read data from Pulsar topics",
    icon = "pulsar.png",
    recordsByRef = true,
    upgraderDef = "upgrader/PulsarDSource.yaml",
    onlineHelpRefUrl = "index.html?contextID=task_kzh_cpc_r2b",
    services = @ServiceDependency(
        service = DataFormatParserService.class,
        configuration = {
            @ServiceConfiguration(name = "displayFormats", value = "BINARY,DATAGRAM,DELIMITED,JSON,LOG,PROTOBUF," +
                "SDC_JSON,TEXT,XML")
        }
    )
)
@ConfigGroups(PulsarGroups.class)
@GenerateResourceBundle
public class PulsarDSource extends DSourceOffsetCommitter {

  @ConfigDefBean(groups = {"PULSAR"})
  public BasicConfig basicConfig;

  @ConfigDefBean(groups = {"PULSAR"})
  public MessageConfig messageConfig;

  @ConfigDefBean
  public PulsarSourceConfig pulsarConfig;


  @Override
  protected Source createSource() {
    return new PulsarSource(basicConfig, pulsarConfig, new PulsarMessageConsumerFactoryImpl(),
        new PulsarMessageConverterImpl(messageConfig));
  }
}
