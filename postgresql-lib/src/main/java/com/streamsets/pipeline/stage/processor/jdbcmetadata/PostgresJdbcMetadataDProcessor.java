/*
 * Copyright 2019 StreamSets Inc.
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
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.lib.jdbc.EncryptionGroups;

//@StageDef(
//    version = 1,
//    label = "PostgreSQL Metadata",
//    description = "Create/alter tables in PostgreSQL to match record structure",
//    icon = "postgresql.png",
//    onlineHelpRefUrl ="index.html?contextID=task_lpv_zsh_qcb"
//)
@ConfigGroups(value = EncryptionGroups.class)
@GenerateResourceBundle
@HideConfigs(value = {
    "conf.hikariConfigBean.readOnly", "conf.hikariConfigBean.autoCommit",
})
public class PostgresJdbcMetadataDProcessor extends JdbcMetadataDProcessor {
  @ConfigDefBean
  public JdbcMetadataConfigBean conf;

  @Override
  protected Processor createProcessor() {
    return new JdbcMetadataProcessor(conf);
  }
}
