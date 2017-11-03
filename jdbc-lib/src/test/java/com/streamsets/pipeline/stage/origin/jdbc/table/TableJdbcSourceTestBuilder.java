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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.streamsets.pipeline.lib.jdbc.ConnectionPropertyBean;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.multithread.BatchTableStrategy;
import com.streamsets.pipeline.lib.jdbc.multithread.TableOrderStrategy;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TableJdbcSourceTestBuilder {
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
  private List<TableConfigBean> tableConfigBeanList;
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


  public TableJdbcSourceTestBuilder(String jdbcUrl, boolean useCredentials, String username, String password) {
    this.connectionString = jdbcUrl;
    this.useCredentials = useCredentials;
    this.username = username;
    this.password = password;
    this.driverProperties = new ArrayList<>();
    this.driverClassName = "";
    this.connectionTestQuery = "";
    this.queriesPerSecond = "0";
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

  public TableJdbcSourceTestBuilder() {
   this("", false, "", "");
  }

  public TableJdbcSourceTestBuilder connectionString(String connectionString) {
    this.connectionString = connectionString;
    return this;
  }

  public TableJdbcSourceTestBuilder useCredentials(boolean useCredentials) {
    this.useCredentials = useCredentials;
    return this;
  }

  public TableJdbcSourceTestBuilder password(String password) {
    this.username = password;
    return this;
  }

  public TableJdbcSourceTestBuilder driverProperties(Map<String, String> driverProperties) {
    for (Map.Entry<String,String> entry : driverProperties.entrySet()) {
      ConnectionPropertyBean config = new ConnectionPropertyBean();
      config.key = entry.getKey();
      config.value = () -> entry.getValue();
      this.driverProperties.add(config);
    }
    return this;
  }

  public TableJdbcSourceTestBuilder driverClassName(String driverClassName) {
    this.driverClassName = driverClassName;
    return this;
  }

  public TableJdbcSourceTestBuilder connectionTestQuery(String connectionTestQuery) {
    this.connectionTestQuery = connectionTestQuery;
    return this;
  }

  public TableJdbcSourceTestBuilder queriesPerSecond(String queriesPerSecond) {
    this.queriesPerSecond = queriesPerSecond;
    return this;
  }

  public TableJdbcSourceTestBuilder maxBatchSize(int maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
    return this;
  }

  public TableJdbcSourceTestBuilder maxClobSize(int maxClobSize) {
    this.maxClobSize = maxClobSize;
    return this;
  }

  public TableJdbcSourceTestBuilder maxBlobSize(int maxBlobSize) {
    this.maxBlobSize = maxBlobSize;
    return this;
  }

  public TableJdbcSourceTestBuilder numSQLErrorRetries(int numSQLErrorRetries) {
    this.numSQLErrorRetries = numSQLErrorRetries;
    return this;
  }

  public TableJdbcSourceTestBuilder tableConfigBeans(List<TableConfigBean> tableConfigBeans) {
    this.tableConfigBeanList.addAll(tableConfigBeans);
    return this;
  }

  public TableJdbcSourceTestBuilder tableConfigBean(TableConfigBean tableConfigBean) {
    this.tableConfigBeanList.add(tableConfigBean);
    return this;
  }

  public TableJdbcSourceTestBuilder timeZoneID(String timeZoneID) {
    this.timeZoneID = timeZoneID;
    return this;
  }

  public TableJdbcSourceTestBuilder batchTableStrategy(BatchTableStrategy batchTableStrategy) {
    this.batchTableStrategy = batchTableStrategy;
    return this;
  }

  public TableJdbcSourceTestBuilder tableOrderStrategy(TableOrderStrategy tableOrderStrategy) {
    this.tableOrderStrategy = tableOrderStrategy;
    return this;
  }

  public TableJdbcSourceTestBuilder resultSetCacheSize(int resultSetCacheSize) {
    this.resultSetCacheSize = resultSetCacheSize;
    return this;
  }

  public TableJdbcSourceTestBuilder fetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
    return this;
  }

  public TableJdbcSourceTestBuilder quoteChar(QuoteChar quoteChar) {
    this.quoteChar = quoteChar;
    return this;
  }

  public TableJdbcSourceTestBuilder numberOfBatchesFromResultset(int numberOfBatchesFromResultset) {
    this.numberOfBatchesFromResultset = numberOfBatchesFromResultset;
    return this;
  }

  public TableJdbcSourceTestBuilder numberOfThreads(int numberOfThreads) {
    this.numberOfThreads = numberOfThreads;
    return this;
  }

  public TableJdbcSourceTestBuilder maximumPoolSize(int maximumPoolSize) {
    this.maximumPoolSize = maximumPoolSize;
    return this;
  }

  public TableJdbcSource build() {
    HikariPoolConfigBean hikariPoolConfigBean = new HikariPoolConfigBean();
    hikariPoolConfigBean.useCredentials = useCredentials;
    hikariPoolConfigBean.connectionString = connectionString;
    hikariPoolConfigBean.username = () -> username;
    hikariPoolConfigBean.password = () -> password;
    hikariPoolConfigBean.driverClassName = driverClassName;
    hikariPoolConfigBean.driverProperties = driverProperties;
    hikariPoolConfigBean.connectionTestQuery = connectionTestQuery;
    if (maximumPoolSize == -1) {
      hikariPoolConfigBean.maximumPoolSize = numberOfThreads + 1;
    }

    TableJdbcConfigBean tableJdbcConfigBean = new TableJdbcConfigBean();
    tableJdbcConfigBean.tableConfigs = tableConfigBeanList;
    tableJdbcConfigBean.fetchSize = fetchSize;
    tableJdbcConfigBean.tableOrderStrategy = tableOrderStrategy;
    tableJdbcConfigBean.timeZoneID = timeZoneID;
    tableJdbcConfigBean.batchTableStrategy = batchTableStrategy;
    tableJdbcConfigBean.resultCacheSize = resultSetCacheSize;
    tableJdbcConfigBean.numberOfThreads = numberOfThreads;
    tableJdbcConfigBean.numberOfBatchesFromRs = numberOfBatchesFromResultset;
    tableJdbcConfigBean.quoteChar = quoteChar;

    CommonSourceConfigBean commonSourceConfigBean =  new CommonSourceConfigBean(
        queriesPerSecond,
        maxBatchSize,
        maxClobSize,
        maxBlobSize
    );

    commonSourceConfigBean.numSQLErrorRetries = numSQLErrorRetries;

    return new TableJdbcSource(
        hikariPoolConfigBean,
        commonSourceConfigBean,
        tableJdbcConfigBean
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
    private PartitioningMode partitioningMode;
    private String partitionSize;
    private int maxNumActivePartitions;
    private boolean enableNonIncremental;

    public TableConfigBeanTestBuilder() {
      this.schema = "";
      this.tablePattern = "%";
      this.tableExclusionPattern = "";
      this.overrideDefaultOffsetColumns = false;
      this.offsetColumns = new ArrayList<>();
      this.offsetColumnToInitialOffsetValue = Collections.emptyMap();
      this.extraOffsetColumnConditions = "";
      this.partitioningMode = TableConfigBean.PARTITIONING_MODE_DEFAULT_VALUE;
      this.partitionSize = TableConfigBean.DEFAULT_PARTITION_SIZE;
      this.maxNumActivePartitions = TableConfigBean.DEFAULT_MAX_NUM_ACTIVE_PARTITIONS;
      this.enableNonIncremental = TableConfigBean.ENABLE_NON_INCREMENTAL_DEFAULT_VALUE;
    }

    public TableConfigBeanTestBuilder schema(String schema) {
      this.schema = schema;
      return this;
    }

    public TableConfigBeanTestBuilder tablePattern(String tablePattern) {
      this.tablePattern = tablePattern;
      return this;
    }

    public TableConfigBeanTestBuilder tableExclusionPattern(String tableExclusionPattern) {
      this.tableExclusionPattern = tableExclusionPattern;
      return this;
    }

    public TableConfigBeanTestBuilder overrideDefaultOffsetColumns(boolean overrideDefaultOffsetColumns) {
      this.overrideDefaultOffsetColumns = overrideDefaultOffsetColumns;
      return this;
    }

    public TableConfigBeanTestBuilder offsetColumns(List<String> offsetColumns) {
      this.offsetColumns = offsetColumns;
      return this;
    }

    public TableConfigBeanTestBuilder offsetColumnToInitialOffsetValue(Map<String, String> offsetColumnToInitialOffsetValue) {
      this.offsetColumnToInitialOffsetValue = offsetColumnToInitialOffsetValue;
      return this;
    }

    public TableConfigBeanTestBuilder extraOffsetColumnConditions(String extraOffsetColumnConditions) {
      this.extraOffsetColumnConditions = extraOffsetColumnConditions;
      return this;
    }

    public TableConfigBeanTestBuilder partitioningMode(PartitioningMode partitioningMode) {
      this.partitioningMode = partitioningMode;
      return this;
    }

    public TableConfigBeanTestBuilder enableNonIncremental(boolean enableNonIncremental) {
      this.enableNonIncremental = enableNonIncremental;
      return this;
    }

    public TableConfigBeanTestBuilder partitionSize(String partitionSize) {
      this.partitionSize = partitionSize;
      return this;
    }

    public TableConfigBeanTestBuilder maxNumActivePartitions(int maxNumActivePartitions) {
      this.maxNumActivePartitions = maxNumActivePartitions;
      return this;
    }

    public TableConfigBean build() {
      TableConfigBean tableConfigBean = new TableConfigBean();
      tableConfigBean.schema = schema;
      tableConfigBean.tablePattern = tablePattern;
      tableConfigBean.tableExclusionPattern = tableExclusionPattern;
      tableConfigBean.offsetColumnToInitialOffsetValue = offsetColumnToInitialOffsetValue;
      tableConfigBean.overrideDefaultOffsetColumns = overrideDefaultOffsetColumns;
      tableConfigBean.offsetColumns = offsetColumns;
      tableConfigBean.extraOffsetColumnConditions = extraOffsetColumnConditions;
      tableConfigBean.partitioningMode = partitioningMode;
      tableConfigBean.enableNonIncremental = enableNonIncremental;
      tableConfigBean.partitionSize = partitionSize;
      tableConfigBean.maxNumActivePartitions = maxNumActivePartitions;
      return tableConfigBean;
    }
  }
}
