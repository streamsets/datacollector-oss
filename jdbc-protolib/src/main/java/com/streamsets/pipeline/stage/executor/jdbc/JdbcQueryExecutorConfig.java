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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcHikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class JdbcQueryExecutorConfig {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcQueryExecutorConfig.class);

  @ConfigDefBean()
  public JdbcHikariPoolConfigBean hikariConfigBean;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.LIST,
      mode = ConfigDef.Mode.SQL,
      label = "SQL Queries",
      description = "SQL queries to execute for each incoming record.",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = { RecordEL.class },
      displayPosition = 20,
      group = "JDBC"
  )
  public List<String> queries;
  //Since the name of the variable is used to define the EL functions
  private static final String QUERIES_VARIABLE_NAME = "queries";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      label = "Include Query Result Count in Events",
      description = "Include the number of rows impacted by a query in generated event records.",
      defaultValue = "false",
      displayPosition = 40,
      group = "JDBC"
  )
  public boolean queryResultCount = false;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Batch Commit",
      description = "Commit changes to database after each batch.",
      defaultValue = "true",
      displayPosition = 52,
      group = "ADVANCED"
  )
  public boolean batchCommit = true;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Enable Parallel Queries",
      description = "Execute multiple queries simultaneously.  Within a batch, " +
          "there can be up to three phases. " +
          "Records are reordered to process all inserts first, all updates second (in original order), and all " +
          "deletes last.",
      displayPosition = 25,
      group = "ADVANCED"
  )
  public boolean isParallel = false;

  private HikariDataSource dataSource = null;

  public String getQueriesVariableName() {
    return QUERIES_VARIABLE_NAME;
  }

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    overrideDefaultValues();
    issues.addAll(getHikariConfigBean().validateConfigs(context, issues));
    addDriverAdditionalProperties();

    if(issues.isEmpty()) {
      try {
        dataSource = UtilsProvider.getJdbcUtil().createDataSourceForRead(getHikariConfigBean());
      } catch (StageException e) {
        LOG.error("Can't open connection", e);
        issues.add(
            context.createConfigIssue(
                Groups.JDBC.name(),
                "config.query",
                QueryExecErrors.QUERY_EXECUTOR_002,
                e.getMessage()
            )
        );
      }
    }
  }

  public void destroy() {
    if(dataSource != null) {
      dataSource.close();
    }
  }

  public Connection getConnection() throws SQLException {
    if (dataSource != null) {
      return dataSource.getConnection();
    }
    return null;
  }

  protected void addDriverAdditionalProperties() {
    getHikariConfigBean().addExtraDriverProperties(new HashMap<>());
  }

  protected void overrideDefaultValues(){
    //Empty method to be overrided and be able to change defaulted values in HikariConfigBean
  }

  public List<String> getQueries() {
    return queries;
  }

  public boolean isBatchCommit() {
    return batchCommit;
  }

  public void setBatchCommit(boolean batchCommit) {
    this.batchCommit = batchCommit;
  }

  public HikariPoolConfigBean getHikariConfigBean() {
    return hikariConfigBean;
  }

  public boolean isParallel() {
    return isParallel;
  }
}
