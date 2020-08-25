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
package com.streamsets.pipeline.stage.bigquery.origin;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSource;
import com.streamsets.pipeline.stage.bigquery.lib.Groups;

@StageDef(
    version = 3,
    label = "Google BigQuery",
    description = "Executes a query job and processes the result from Google BigQuery",
    icon="bigquery.png",
    execution = ExecutionMode.STANDALONE,
    producesEvents = true,
    recordsByRef = true,
    eventDefs = {BigQuerySuccessEvent.class},
    upgrader = BigQuerySourceUpgrader.class,
    upgraderDef = "upgrader/BigQueryDSource.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_n5w_ykv_q1b"
)
@ConfigGroups(Groups.class)
public class BigQueryDSource extends DSource {
  @ConfigDefBean
  public BigQuerySourceConfig conf = new BigQuerySourceConfig();

  @Override
  protected Source createSource() {
    return new BigQuerySource(conf);
  }
}
