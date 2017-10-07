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
package com.streamsets.pipeline.stage.destination.hive.queryexecutor;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;
import com.streamsets.pipeline.stage.lib.hive.HiveConfigBean;

import java.util.List;

/**
 */
public class HiveQueryExecutorConfig {

  @ConfigDefBean
  public HiveConfigBean hiveConfigBean;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "SQL Queries",
      description = "Queries that will be executed on Hive or Impala.",
      defaultValue = "",
      displayPosition = 40,
      group = "QUERY",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class, StringEL.class}
  )
  public List<String> queries;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Stop On Query Failure",
      description = "Determines whether to stop executing the subsequent queries if there is a failure per record",
      displayPosition = 50,
      group = "QUERY",
      defaultValue = "true"
  )
  public boolean stopOnQueryFailure = true;

  public void init(Stage.Context context, String prefix, List<Stage.ConfigIssue> issues) {
    hiveConfigBean.init(context, prefix, issues);
  }

  public void destroy() {
    hiveConfigBean.destroy();
  }

}
