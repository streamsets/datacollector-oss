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
package com.streamsets.datacollector.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

/**
 * Enum for representing possible Cluster Type
 */
@GenerateResourceBundle
public enum SparkClusterType implements Label {
  AZURE_HD_INSIGHT("Apache Spark for HDInsight"),
  DATABRICKS("Databricks"),
  EMR("EMR"),
  DATAPROC("Dataproc"),
  YARN("Hadoop YARN"),
  KUBERNETES("Kubernetes Cluster"),
  LOCAL("None (local)"),
  STANDALONE_SPARK_CLUSTER("Spark Standalone Cluster"),
  SQL_SERVER_BIG_DATA_CLUSTER("SQL Server 2019 Big Data Cluster"),
  ;

  private final String label;

  SparkClusterType(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
