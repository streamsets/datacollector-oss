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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class BigQueryTargetConfigBuilder {
  private String projectId;
  private String datasetEL;
  private String tableNameEL;

  private boolean implicitFieldMapping;
  private boolean ignoreInvalidColumns;
  private Map<String, String> columnToFieldMapping;

  BigQueryTargetConfigBuilder() {
    this.implicitFieldMapping = false;
    this.ignoreInvalidColumns = true;
    this.columnToFieldMapping = new LinkedHashMap<>();
    this.datasetEL = "sample";
    this.tableNameEL = "sample";
    this.projectId = "sample";
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

  public BigQueryTargetConfigBuilder implicitFieldMapping(boolean implicitFieldMapping) {
    this.implicitFieldMapping = implicitFieldMapping;
    return this;
  }

  public BigQueryTargetConfigBuilder ignoreInvalidColumns(boolean ignoreInvalidColumns) {
    this.ignoreInvalidColumns = ignoreInvalidColumns;
    return this;
  }

  public BigQueryTargetConfigBuilder columnToFieldNameMapping(Map<String, String> columnToFieldMapping) {
    this.columnToFieldMapping =  columnToFieldMapping;
    return this;
  }

  public BigQueryTargetConfig build() throws Exception {
    BigQueryTargetConfig config = new BigQueryTargetConfig();
    config.credentials.projectId = projectId;
    config.datasetEL = datasetEL;
    config.tableNameEL = tableNameEL;
    config.ignoreInvalidColumn = ignoreInvalidColumns;
    config.implicitFieldMapping = implicitFieldMapping;
    config.bigQueryFieldMappingConfigs =
        columnToFieldMapping.entrySet().stream().map(e ->  {
          BigQueryFieldMappingConfig fieldMappingConfig = new BigQueryFieldMappingConfig();
          fieldMappingConfig.columnName = e.getKey();
          fieldMappingConfig.fieldPath = e.getValue();
          return fieldMappingConfig;
        }).collect(Collectors.toList());
    return config;
  }

}
