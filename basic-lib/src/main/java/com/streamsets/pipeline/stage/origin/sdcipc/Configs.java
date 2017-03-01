/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.sdcipc;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.el.VaultEL;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class Configs extends HttpConfigs {
  private static final Logger LOG = LoggerFactory.getLogger(Configs.class);

  private static final String CONFIG_PREFIX = "config.";
  private static final String PORT = CONFIG_PREFIX + "port";
  private static final String KEY_STORE_FILE = CONFIG_PREFIX + "keyStoreFile";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "20000",
      label = "SDC RPC Listening Port",
      description = "Port number to listen for data. Must match one of the port numbers used by the SDC RPC destination of the origin pipeline.",
      displayPosition = 10,
      group = "RPC",
      min = 1,
      max = 65535
  )
  public int port;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "SDC RPC ID",
      description = "User-defined ID. Must match the SDC RPC ID used by the SDC RPC destination of the origin pipeline.",
      displayPosition = 20,
      elDefs = VaultEL.class,
      group = "RPC"
  )
  public String appId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5",
      label = "Batch Wait Time (secs)",
      description = " Maximum amount of time to wait for a batch before sending and empty one",
      displayPosition = 30,
      group = "RPC",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxWaitTimeSecs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "TLS Enabled",
      description = "Encrypt RPC communication using TLS.",
      displayPosition = 40,
      group = "RPC"
  )
  public boolean sslEnabled;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Keystore File",
      description = "The keystore file is expected in the Data Collector resources directory",
      displayPosition = 50,
      group = "RPC",
      dependsOn = "sslEnabled",
      triggeredByValue = "true"
  )
  public String keyStoreFile;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Keystore Password",
      displayPosition = 60,
      elDefs = VaultEL.class,
      group = "RPC",
      dependsOn = "sslEnabled",
      triggeredByValue = "true"
  )
  public String keyStorePassword;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10",
      label = "Max Record Size (MB)",
      description = "",
      displayPosition = 10,
      group = "ADVANCED",
      min = 1,
      max = 100
  )
  public int maxRecordSize;

  File getKeyStoreFile(Stage.Context context) {
    return new File(context.getResourcesDirectory(), keyStoreFile);
  }

  public Configs() {
    super("RPC", "config.");
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public int getMaxConcurrentRequests() {
    return 100;
  }

  @Override
  public String getAppId() {
    return appId;
  }

  @Override
  public int getMaxHttpRequestSizeKB() {
    return 10000;
  }

  @Override
  public boolean isSslEnabled() {
    return sslEnabled;
  }

  @Override
  public boolean isAppIdViaQueryParamAllowed() {
    return false;
  }

  @Override
  public String getKeyStoreFile() {
    return keyStoreFile;
  }

  @Override
  public String getKeyStorePassword() {
    return keyStorePassword;
  }
}
