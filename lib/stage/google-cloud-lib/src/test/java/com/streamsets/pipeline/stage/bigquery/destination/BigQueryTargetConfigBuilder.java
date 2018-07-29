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
package com.streamsets.pipeline.stage.bigquery.destination;

import com.streamsets.pipeline.stage.lib.CredentialsProviderType;

public class BigQueryTargetConfigBuilder {
  private String projectId;
  private String datasetEL;
  private String tableNameEL;
  private String rowIdExpression;

  private boolean ignoreInvalidColumns;

  BigQueryTargetConfigBuilder() {
    this.ignoreInvalidColumns = true;
    this.datasetEL = "correctDataset";
    this.tableNameEL = "correctTable";
    this.projectId = "sample";
    this.rowIdExpression = "";
  }

  public BigQueryTargetConfigBuilder projectId(String projectId) {
    this.projectId = projectId;
    return this;
  }

  public BigQueryTargetConfigBuilder datasetEL(String datasetEL) {
    this.datasetEL = datasetEL;
    return this;
  }

  public BigQueryTargetConfigBuilder tableNameEL(String tableNameEL) {
    this.tableNameEL = tableNameEL;
    return this;
  }

  public BigQueryTargetConfigBuilder rowIdExpression(String rowIdExpression) {
    this.rowIdExpression = rowIdExpression;
    return this;
  }

  public BigQueryTargetConfigBuilder ignoreInvalidColumns(boolean ignoreInvalidColumns) {
    this.ignoreInvalidColumns = ignoreInvalidColumns;
    return this;
  }

  public BigQueryTargetConfig build() throws Exception {
    BigQueryTargetConfig config = new BigQueryTargetConfig();
    config.credentials.projectId = projectId;
    config.datasetEL = datasetEL;
    config.tableNameEL = tableNameEL;
    config.ignoreInvalidColumn = ignoreInvalidColumns;
    config.rowIdExpression = rowIdExpression;
    config.credentials.credentialsProvider = CredentialsProviderType.JSON_PROVIDER;
    return config;
  }

}
