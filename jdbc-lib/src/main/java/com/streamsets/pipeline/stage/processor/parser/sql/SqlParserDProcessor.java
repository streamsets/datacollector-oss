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
package com.streamsets.pipeline.stage.processor.parser.sql;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

@StageDef(
    version = 1,
    label = "SQL Parser",
    description = "Processor that can parse SQL insert, update, and delete queries",
    icon = "sql.png",
    recordsByRef = true,
    upgraderDef = "upgrader/SqlParserDProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_pfr_wdc_tdb"
)
@GenerateResourceBundle
@ConfigGroups(Groups.class)
public class SqlParserDProcessor extends DProcessor {

  @ConfigDefBean
  public SqlParserConfigBean configBean = new SqlParserConfigBean();

  @Override
  protected Processor createProcessor() {
    return new SqlParserProcessor(configBean);
  }
}
