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
package com.streamsets.pipeline.stage.processor.jdbcmetadata;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DProcessor;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;

@StageDef(
    version = 2,
    label = "JDBC Metadata",
    description = "Write metadata to JDBC and write information for JDBC",
    icon = "rdbms.png",
    onlineHelpRefUrl = "index.html#TBD"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
@HideConfigs(value = {
  "conf.hikariConfigBean.readOnly",
  "conf.hikariConfigBean.autoCommit"
})
public class JdbcMetadataDProcessor extends DProcessor {
  @ConfigDefBean
  public JdbcMetadataConfigBean conf;

  @Override
  protected Processor createProcessor() {
    return new JdbcMetadataProcessor(conf);
  }
}
