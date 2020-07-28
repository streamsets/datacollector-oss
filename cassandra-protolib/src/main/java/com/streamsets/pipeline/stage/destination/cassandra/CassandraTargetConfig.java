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
package com.streamsets.pipeline.stage.destination.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolVersion;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;

import java.util.ArrayList;
import java.util.List;

public class CassandraTargetConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      defaultValue = "[\"localhost\"]",
      label = "Cassandra Contact Points",
      description = "Hostnames of Cassandra nodes to use as contact points. To ensure a connection, enter several.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CASSANDRA"
  )
  public List<String> contactPoints = new ArrayList<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "9042",
      label = "Cassandra Port",
      description = "Port number to use when connecting to Cassandra nodes",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CASSANDRA"
  )
  public int port = 9042;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Authentication Provider",
      defaultValue = "NONE",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CASSANDRA"
  )
  @ValueChooserModel(AuthenticatorClassChooserValues.class)
  public AuthProviderOption authProviderOption = AuthProviderOption.NONE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Protocol Version",
      description = "If unsure which setting to use, refer to: https://datastax.github" +
          ".io/java-driver/manual/native_protocol",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CASSANDRA"
  )
  @ValueChooserModel(ProtocolVersionChooserValues.class)
  public ProtocolVersion protocolVersion;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "LZ4",
      label = "Compression",
      description = "Optional compression for transport-level requests and responses.",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "CASSANDRA"
  )
  @ValueChooserModel(CompressionChooserValues.class)
  public CassandraCompressionCodec compression = CassandraCompressionCodec.LZ4;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Enable Batches",
      description = "Enables the use of Cassandra batches",
      displayPosition = 51,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "CASSANDRA"
  )
  public boolean enableBatches = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000", // from SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS
      min = 1,
      max = Integer.MAX_VALUE,
      label = "Write Timeout",
      description = "The timeout for each write request (in milliseconds)",
      displayPosition = 52,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "CASSANDRA",
      dependsOn = "enableBatches",
      triggeredByValue = "false"
  )
  public int writeTimeout = 5000;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "LOGGED",
      label = "Batch Type",
      description = "Un-logged batches do not use the Cassandra distributed batch log and as such as nonatomic.",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "CASSANDRA",
      dependsOn = "enableBatches",
      triggeredByValue = "true"
  )
  @ValueChooserModel(BatchTypeChooserValues.class)
  public BatchStatement.Type batchType = BatchStatement.Type.LOGGED;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "65535",
      min = 1,
      max = 65535,
      label = "Max Batch Size",
      description = "Maximum statements to batch prior to submission.",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "CASSANDRA"
  )
  public int maxBatchSize = 65535;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Fully Qualified Table Name",
      description = "Table write to, e.g. <keyspace>.<table_name>",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CASSANDRA"
  )
  public String qualifiedTableName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Field to Column Mapping",
      description = "Fields to map to Cassandra columns. To avoid errors, field data types must match.",
      displayPosition = 90,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CASSANDRA"
  )
  @ListBeanModel
  public List<CassandraFieldMappingConfig> columnNames;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000", // from SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS
      min = 1,
      max = Integer.MAX_VALUE,
      label = "Connection Timeout",
      description = "The connection timeout (in milliseconds)",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "CASSANDRA"
  )
  public int connectionTimeout = 5000;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000", // from SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS
      min = 1,
      max = Integer.MAX_VALUE,
      label = "Read Timeout",
      description = "The per-host read timeout (in milliseconds)",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "CASSANDRA"
  )
  public int readTimeout = 5000;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "LOCAL_ONE",
      label = "Consistency Level",
      description = "The consistency level to use for queries",
      displayPosition = 120,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "CASSANDRA"
  )
  @ValueChooserModel(ConsistencyLevelChooserValues.class)
  public ConsistencyLevel consistencyLevel = ConsistencyLevel.LOCAL_ONE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Log Slow Queries",
      description = "Enables the logging of slow queries. " +
          "Note that the logger for com.datastax.driver.core.QueryLogger.SLOW must be set to either DEBUG or TRACE.",
      displayPosition = 130,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "CASSANDRA"
  )
  public boolean logSlowQueries = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000",  // from QueryLogger.DEFAULT_SLOW_QUERY_THRESHOLD_MS
      min = 1,
      label = "Slow Query Logging Threshold",
      description = "The threshold (in milliseconds) to consider a query slow",
      displayPosition = 140,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "CASSANDRA",
      dependsOn = "logSlowQueries",
      triggeredByValue = "true"
  )
  public long slowQueryThreshold = 5000;

  /** Credentials group **/
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CREDENTIALS",
      dependsOn = "authProviderOption",
      triggeredByValue = {"PLAINTEXT", "DSE_PLAINTEXT"}
  )
  public CredentialValue username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      defaultValue = "",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CREDENTIALS",
      dependsOn = "authProviderOption",
      triggeredByValue = {"PLAINTEXT", "DSE_PLAINTEXT"}
  )
  public CredentialValue password;

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfig = new TlsConfigBean();
}
