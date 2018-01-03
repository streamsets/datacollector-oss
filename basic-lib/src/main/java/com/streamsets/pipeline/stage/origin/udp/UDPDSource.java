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

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.configurablestage.DSource;

@StageDef(
    version = 4,
    label = "UDP Source",
    description = "Listens for UDP messages on one or more ports",
    icon = "udp.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    upgrader = UDPSourceUpgrader.class,
    onlineHelpRefUrl = "index.html#Origins/UDP.html#task_kgn_rcv_1s"
)

@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class UDPDSource extends DSource {
  public static final String CONFIG_PREFIX = "conf.";

  @ConfigDefBean
  public UDPSourceConfigBean conf;

  @Override
  protected Source createSource() {
    Utils.checkNotNull(conf.dataFormat, "Data format cannot be null");
    Utils.checkNotNull(conf.ports, "Ports cannot be null");

    return new UDPSource(conf
    );
  }
}
