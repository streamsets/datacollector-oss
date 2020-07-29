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
package com.streamsets.pipeline.lib.couchbase;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;

import java.util.List;

public class CouchbaseConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost",
      label = "Node List",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "A comma-separated list of one or more Couchbase Cluster nodes",
      group = "COUCHBASE"
  )
  public String nodes;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Bucket",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      description = "Name of the Couchbase bucket to write to",
      group = "COUCHBASE"
  )
  public String bucket;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2500",
      min = 1,
      label = "Key-Value Timeout (ms)",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      description = "Maximum execution time for each key/value operation",
      group = "COUCHBASE"
  )
  public long kvTimeout = 2500;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000",
      min = 1,
      label = "Connect Timeout (ms)",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      description = "Maximum time to wait for connect operations",
      group = "COUCHBASE"
  )
  public long connectTimeout = 5000;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "25000",
      min = 1,
      label = "Disconnect Timeout (ms)",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      description = "Maximum grace period for flushing async operations before a " +
          "bucket is closed",
      group = "COUCHBASE"
  )
  public long disconnectTimeout = 2500;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Advanced Environment Settings",
      description = "CouchbaseEnvironment settings. Consult the Couchbase Java SDK documentation for " +
          "available parameters",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "COUCHBASE"
  )
  @ListBeanModel
  public List<EnvConfig> envConfigs;

  @ConfigDefBean(groups = "COUCHBASE")
  public TlsConfigBean tls = new TlsConfigBean();

}
