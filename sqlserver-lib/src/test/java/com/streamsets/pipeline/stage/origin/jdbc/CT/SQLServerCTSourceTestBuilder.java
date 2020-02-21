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
package com.streamsets.pipeline.stage.origin.jdbc.CT;

import com.streamsets.pipeline.lib.jdbc.ConnectionPropertyBean;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.multithread.BatchTableStrategy;
import com.streamsets.pipeline.lib.jdbc.multithread.TableOrderStrategy;
import com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.CTTableConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.CTTableJdbcConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.SQLServerCTSource;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.QuoteChar;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBeanImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SQLServerCTSourceTestBuilder {
  private String connectionString;
  private boolean useCredentials;
  private String username;
  private String password;
  private List<ConnectionPropertyBean> driverProperties;
  private  String driverClassName;
  private String connectionTestQuery;
  private String queriesPerSecond;
  private int maxBatchSize;
  private int maxClobSize;
  private int maxBlobSize;
  private List<CTTableConfigBean> tableConfigBeanList;
  private String timeZoneID;
  private int fetchSize;
  private BatchTableStrategy batchTableStrategy;
  private TableOrderStrategy tableOrderStrategy;
  private int resultSetCacheSize;
  private int numberOfThreads;
  private int maximumPoolSize;
  private int numberOfBatchesFromResultset;
  private QuoteChar quoteChar;
  private int numSQLErrorRetries;


  public SQLServerCTSourceTestBuilder(String jdbcUrl, boolean useCredentials, String username, String password) {
    this.connectionString = jdbcUrl;
    this.useCredentials = useCredentials;
    this.username = username;
    this.password = password;
    this.driverProperties = new ArrayList<>();
    this.driverClassName = "";
    this.connectionTestQuery = "";
    this.queriesPerSecond = "1";
    this.maxBatchSize = 1000;
    this.maxClobSize = 1000;
    this.maxBlobSize = 1000;
    this.tableConfigBeanList = new ArrayList<>();
    this.timeZoneID = "UTC";
    this.fetchSize = 1000;
    this.batchTableStrategy = BatchTableStrategy.SWITCH_TABLES;
    this.tableOrderStrategy = TableOrderStrategy.NONE;
    this.resultSetCacheSize = -1;
    this.numberOfThreads = 1;
    this.maximumPoolSize = -1;
    this.numberOfBatchesFromResultset = -1;
    this.quoteChar = QuoteChar.NONE;
    this.numSQLErrorRetries = 0;
  }

  public SQLServerCTSourceTestBuilder() {
    this("", false, "", "");
  }

  public SQLServerCTSourceTestBuilder connectionString(String connectionString) {
    this.connectionString = connectionString;
    return this;
  }

  public SQLServerCTSourceTestBuilder useCredentials(boolean useCredentials) {
    this.useCredentials = useCredentials;
    return this;
  }

  public SQLServerCTSourceTestBuilder password(String password) {
    this.username = password;
    return this;
  }

  public SQLServerCTSourceTestBuilder driverProperties(Map<String, String> driverProperties) {
    for (Map.Entry<String,String> entry : driverProperties.entrySet()) {
      ConnectionPropertyBean config = new ConnectionPropertyBean();
      config.key = entry.getKey();
      config.value = () -> entry.getValue();
      this.driverProperties.add(config);
    }
    return this;
  }

  public SQLServerCTSourceTestBuilder driverClassName(String driverClassName) {
    this.driverClassName = driverClassName;
    return this;
  }

  public SQLServerCTSourceTestBuilder connectionTestQuery(String connectionTestQuery) {
    this.connectionTestQuery = connectionTestQuery;
    return this;
  }

  public SQLServerCTSourceTestBuilder queriesPerSecond(String queriesPerSecond) {
    this.queriesPerSecond = queriesPerSecond;
    return this;
  }

  public SQLServerCTSourceTestBuilder maxBatchSize(int maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
    return this;
  }

  public SQLServerCTSourceTestBuilder maxClobSize(int maxClobSize) {
    this.maxClobSize = maxClobSize;
    return this;
  }

  public SQLServerCTSourceTestBuilder maxBlobSize(int maxBlobSize) {
    this.maxBlobSize = maxBlobSize;
    return this;
  }

  public SQLServerCTSourceTestBuilder numSQLErrorRetries(int numSQLErrorRetries) {
    this.numSQLErrorRetries = numSQLErrorRetries;
    return this;
  }

  public SQLServerCTSourceTestBuilder tableConfigBeans(List<CTTableConfigBean> tableConfigBeans) {
    this.tableConfigBeanList.addAll(tableConfigBeans);
    return this;
  }

  public SQLServerCTSourceTestBuilder tableConfigBean(CTTableConfigBean tableConfigBean) {
    this.tableConfigBeanList.add(tableConfigBean);
    return this;
  }

  public SQLServerCTSourceTestBuilder timeZoneID(String timeZoneID) {
    this.timeZoneID = timeZoneID;
    return this;
  }

  public SQLServerCTSourceTestBuilder batchTableStrategy(BatchTableStrategy batchTableStrategy) {
    this.batchTableStrategy = batchTableStrategy;
    return this;
  }

  public SQLServerCTSourceTestBuilder tableOrderStrategy(TableOrderStrategy tableOrderStrategy) {
    this.tableOrderStrategy = tableOrderStrategy;
    return this;
  }

  public SQLServerCTSourceTestBuilder resultSetCacheSize(int resultSetCacheSize) {
    this.resultSetCacheSize = resultSetCacheSize;
    return this;
  }

  public SQLServerCTSourceTestBuilder quoteChar(QuoteChar quoteChar) {
    this.quoteChar = quoteChar;
    return this;
  }

  public SQLServerCTSourceTestBuilder numberOfBatchesFromResultset(int numberOfBatchesFromResultset) {
    this.numberOfBatchesFromResultset = numberOfBatchesFromResultset;
    return this;
  }

  public SQLServerCTSourceTestBuilder numberOfThreads(int numberOfThreads) {
    this.numberOfThreads = numberOfThreads;
    return this;
  }

  public SQLServerCTSourceTestBuilder maximumPoolSize(int maximumPoolSize) {
    this.maximumPoolSize = maximumPoolSize;
    return this;
  }

  public SQLServerCTSource build() {
    HikariPoolConfigBean hikariPoolConfigBean = new HikariPoolConfigBean();
    hikariPoolConfigBean.useCredentials = useCredentials;
    hikariPoolConfigBean.connectionString = connectionString;
    hikariPoolConfigBean.username = () -> username;
    hikariPoolConfigBean.password = () -> password;
    hikariPoolConfigBean.driverProperties = driverProperties;
    if (maximumPoolSize == -1) {
      hikariPoolConfigBean.maximumPoolSize = numberOfThreads + 1;
    }

    CTTableJdbcConfigBean tableJdbcConfigBean = new CTTableJdbcConfigBean();
    tableJdbcConfigBean.tableConfigs = tableConfigBeanList;
    tableJdbcConfigBean.fetchSize = fetchSize;
    tableJdbcConfigBean.tableOrderStrategy = tableOrderStrategy;
    tableJdbcConfigBean.timeZoneID = timeZoneID;
    tableJdbcConfigBean.batchTableStrategy = batchTableStrategy;
    tableJdbcConfigBean.resultCacheSize = resultSetCacheSize;
    tableJdbcConfigBean.numberOfThreads = numberOfThreads;
    tableJdbcConfigBean.numberOfBatchesFromRs = numberOfBatchesFromResultset;

    CommonSourceConfigBean commonSourceConfigBean =  new CommonSourceConfigBean(
        queriesPerSecond,
        maxBatchSize,
        maxClobSize,
        maxBlobSize
    );

    commonSourceConfigBean.numSQLErrorRetries = numSQLErrorRetries;

    return new SQLServerCTSource(
        hikariPoolConfigBean,
        commonSourceConfigBean,
        tableJdbcConfigBean,
        null
    );
  }

  public static class TableConfigBeanTestBuilder {
    private String schema;
    private String tablePattern;
    private String tableExclusionPattern;
    private boolean overrideDefaultOffsetColumns;
    private List<String> offsetColumns;
    private Map<String, String> offsetColumnToInitialOffsetValue;
    private String extraOffsetColumnConditions;

    public TableConfigBeanTestBuilder() {
      this.schema = "";
      this.tablePattern = "%";
      this.tableExclusionPattern = "";
      this.overrideDefaultOffsetColumns = false;
      this.offsetColumns = new ArrayList<>();
      this.offsetColumnToInitialOffsetValue = Collections.emptyMap();
      this.extraOffsetColumnConditions = "";
    }

    public SQLServerCTSourceTestBuilder.TableConfigBeanTestBuilder schema(String schema) {
      this.schema = schema;
      return this;
    }

    public SQLServerCTSourceTestBuilder.TableConfigBeanTestBuilder tablePattern(String tablePattern) {
      this.tablePattern = tablePattern;
      return this;
    }

    public SQLServerCTSourceTestBuilder.TableConfigBeanTestBuilder tableExclusionPattern(String tableExclusionPattern) {
      this.tableExclusionPattern = tableExclusionPattern;
      return this;
    }

    public SQLServerCTSourceTestBuilder.TableConfigBeanTestBuilder overrideDefaultOffsetColumns(boolean overrideDefaultOffsetColumns) {
      this.overrideDefaultOffsetColumns = overrideDefaultOffsetColumns;
      return this;
    }

    public SQLServerCTSourceTestBuilder.TableConfigBeanTestBuilder offsetColumns(List<String> offsetColumns) {
      this.offsetColumns = offsetColumns;
      return this;
    }

    public SQLServerCTSourceTestBuilder.TableConfigBeanTestBuilder offsetColumnToInitialOffsetValue(Map<String, String> offsetColumnToInitialOffsetValue) {
      this.offsetColumnToInitialOffsetValue = offsetColumnToInitialOffsetValue;
      return this;
    }

    public SQLServerCTSourceTestBuilder.TableConfigBeanTestBuilder extraOffsetColumnConditions(String extraOffsetColumnConditions) {
      this.extraOffsetColumnConditions = extraOffsetColumnConditions;
      return this;
    }

    public TableConfigBean build() {
      TableConfigBeanImpl tableConfigBean = new TableConfigBeanImpl();
      tableConfigBean.schema = schema;
      tableConfigBean.tablePattern = tablePattern;
      tableConfigBean.tableExclusionPattern = tableExclusionPattern;
      tableConfigBean.offsetColumnToInitialOffsetValue = offsetColumnToInitialOffsetValue;
      tableConfigBean.overrideDefaultOffsetColumns = overrideDefaultOffsetColumns;
      tableConfigBean.offsetColumns = offsetColumns;
      tableConfigBean.extraOffsetColumnConditions = extraOffsetColumnConditions;
      return tableConfigBean;
    }
  }

}
