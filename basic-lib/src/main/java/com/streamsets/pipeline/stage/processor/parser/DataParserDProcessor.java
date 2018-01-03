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
package com.streamsets.pipeline.stage.processor.parser;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.configurablestage.DProcessor;

@StageDef(
    version=2,
    label="Data Parser",
    description = "Parses a field with data",
    // from http://www.flaticon.com/free-icon/coding_408290
    icon="coding.png",
    services = @ServiceDependency(
      service = DataFormatParserService.class,
      configuration = {
        @ServiceConfiguration(name = "displayFormats", value = "AVRO,DELIMITED,JSON,LOG,NETFLOW,PROTOBUF,SYSLOG,SDC_JSON,XML")
      }
    ),
    upgrader = DataParserUpgrader.class,
    onlineHelpRefUrl = "index.html#Processors/DataParser.html#task_cx3_2yk_r1b"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class DataParserDProcessor extends DProcessor {

  @ConfigDefBean
  public DataParserConfig configs;

  @Override
  protected Processor createProcessor() {
    return new DataParserProcessor(configs);
  }
}
