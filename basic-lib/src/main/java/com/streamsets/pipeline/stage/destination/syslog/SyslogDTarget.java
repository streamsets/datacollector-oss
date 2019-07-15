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
package com.streamsets.pipeline.stage.destination.syslog;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;

@StageDef(
    version = 2,
    label = "Syslog",
    description = "Writes data to syslog",
    icon = "syslog.png",
    recordsByRef = true,
    onlineHelpRefUrl = "index.html?contextID=task_bbc_pt5_w2b",
    upgrader = SyslogTargetUpgrader.class,
    upgraderDef = "upgrader/SyslogDTarget.yaml",
    services = @ServiceDependency(
        service = DataFormatGeneratorService.class,
        configuration = {
            @ServiceConfiguration(name = "displayFormats",
                value = "AVRO,BINARY,DELIMITED,JSON,PROTOBUF,SDC_JSON,TEXT,XML")
    }
  )
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class SyslogDTarget extends DTarget {

  @ConfigDefBean
  public SyslogTargetConfig config;

  @Override
  protected Target createTarget() { return new SyslogTarget(config); }


}
