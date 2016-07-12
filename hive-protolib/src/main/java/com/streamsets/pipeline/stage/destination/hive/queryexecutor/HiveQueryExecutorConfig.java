/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
      type = ConfigDef.Type.TEXT,
      label = "SQL Query",
      description = "Query that will be executed on Hive or Impala.",
      displayPosition = 40,
      group = "QUERY",
      defaultValue = "invalidate metadata ${record:attribute('/table')}",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class, StringEL.class}
  )
  public String query;

  public void init(Stage.Context context, String prefix, List<Stage.ConfigIssue> issues) {
    hiveConfigBean.init(context, prefix, issues);
  }

  public void destroy() {
    hiveConfigBean.destroy();
  }

}
