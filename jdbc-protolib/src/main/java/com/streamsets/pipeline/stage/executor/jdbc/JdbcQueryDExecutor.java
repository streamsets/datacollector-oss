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
package com.streamsets.pipeline.stage.executor.jdbc;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DExecutor;

@StageDef(
    version = 5,
    label = "JDBC Query",
    description = "Executes queries against JDBC compliant database",
    upgrader = JdbcQueryExecutorUpgrader.class,
    upgraderDef = "upgrader/JdbcQueryDExecutor.yaml",
    icon = "rdbms-executor.png",
    producesEvents = true,
    onlineHelpRefUrl ="index.html?contextID=task_ym2_3cv_sx"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
@HideConfigs(
  "config.hikariConfigBean.readOnly"
)
@PipelineLifecycleStage
public class JdbcQueryDExecutor extends DExecutor {

  @ConfigDefBean
  public JdbcQueryExecutorConfig config;

  /**
   * Returns the JdbcQueryExecutorConfig bean.
   * <p/>
   * This method is used to pass the JdbcQueryExecutorConfig bean to the underlaying connector.
   * <p/>
   * Subclasses may override this method to provide specific vendor configurations.
   * <p/>
   * IMPORTANT: when a subclass is overriding this method to return a specialized JdbcQueryExecutorConfig bean, the
   * config property itself in the connector subclass must have the same name as the config property in this class,
   * this is "config".
   */
  protected JdbcQueryExecutorConfig getQueryExecutorConfigBean() {
    return config;
  }

  @Override
  protected Executor createExecutor() {
    // The executor is meant to issue changing queries so having connection in read only mode doesn't make sense
    config.getHikariConfigBean().readOnly = false;
    return new JdbcQueryExecutor(getQueryExecutorConfigBean());
  }
}
