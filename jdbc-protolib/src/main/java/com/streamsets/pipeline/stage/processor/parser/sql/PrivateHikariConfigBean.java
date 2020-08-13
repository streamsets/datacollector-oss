/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.parser.sql;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.jdbc.BrandedHikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.ConnectionPropertyBean;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.TransactionIsolationLevel;

import java.util.ArrayList;
import java.util.List;

public class PrivateHikariConfigBean {

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "JDBC Connection String",
      displayPosition = 10,
      group = "JDBC",
      dependsOn = "resolveSchema^",
      triggeredByValue = "true"
  )
  public String connectionString = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Use Credentials",
      displayPosition = 11,
      group = "JDBC",
      dependsOn = "resolveSchema^",
      triggeredByValue = "true"
  )
  public boolean useCredentials;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      dependsOn = "useCredentials",
      triggeredByValue = "true",
      label = "Username",
      displayPosition = 110,
      group = "CREDENTIALS"
  )
  public CredentialValue username;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      dependsOn = "useCredentials",
      triggeredByValue = "true",
      label = "Password",
      displayPosition = 120,
      group = "CREDENTIALS"
  )
  public CredentialValue password;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "[]",
      label = "Additional JDBC Configuration Properties",
      description = "Additional properties to pass to the underlying JDBC driver.",
      displayPosition = 999,
      group = "JDBC",
      dependsOn = "resolveSchema^",
      triggeredByValue = "true"
  )
  @ListBeanModel
  public List<ConnectionPropertyBean> driverProperties = new ArrayList<>();

  private BrandedHikariPoolConfigBean underlying;

  public HikariPoolConfigBean getUnderlying() {
    return underlying;
  }

  public List<Stage.ConfigIssue> init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    underlying = new BrandedHikariPoolConfigBean();
    underlying.connectionString = connectionString;
    underlying.useCredentials = useCredentials;
    underlying.username = username;
    underlying.password = password;
    underlying.driverProperties = driverProperties;
    underlying.readOnly = true;
    underlying.setAutoCommit(true);
    underlying.connectionTimeout = HikariPoolConfigBean.DEFAULT_CONNECTION_TIMEOUT;
    underlying.idleTimeout = HikariPoolConfigBean.DEFAULT_IDLE_TIMEOUT;
    underlying.maxLifetime = HikariPoolConfigBean.DEFAULT_MAX_LIFETIME;
    underlying.minIdle = 1;
    underlying.maximumPoolSize = 1;
    underlying.connectionTestQuery = underlying.initialQuery = "select 1 from dual";
    underlying.transactionIsolation = TransactionIsolationLevel.DEFAULT;
    return underlying.validateConfigs(context, issues);
  }

}
