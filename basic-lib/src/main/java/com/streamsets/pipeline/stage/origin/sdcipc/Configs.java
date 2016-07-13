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

import com.streamsets.datacollector.el.VaultEL;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;

public class Configs {
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

  public List<Stage.ConfigIssue> init(Stage.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    validatePort(context, issues);
    validateSecurity(context, issues);
    return issues;
  }

  void validatePort(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (port < 1 || port > 65535) {
      issues.add(context.createConfigIssue(Groups.RPC.name(), PORT, Errors.IPC_ORIG_00));

    } else {
      try (ServerSocket ss = new ServerSocket(port)){
      } catch (Exception ex) {
        issues.add(context.createConfigIssue(Groups.RPC.name(), PORT, Errors.IPC_ORIG_01, ex.toString()));

      }
    }
  }

  void validateSecurity(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (sslEnabled) {
      if (!keyStoreFile.isEmpty()) {
        File file = getKeyStoreFile(context);
        if (!file.exists()) {
          issues.add(context.createConfigIssue(Groups.RPC.name(), KEY_STORE_FILE, Errors.IPC_ORIG_07));
        } else {
          if (!file.isFile()) {
            issues.add(context.createConfigIssue(Groups.RPC.name(), KEY_STORE_FILE, Errors.IPC_ORIG_08));
          } else {
            if (!file.canRead()) {
              issues.add(context.createConfigIssue(Groups.RPC.name(), KEY_STORE_FILE, Errors.IPC_ORIG_09));
            } else {
              try {
                KeyStore keystore = KeyStore.getInstance("jks");
                try (InputStream is = new FileInputStream(getKeyStoreFile(context))) {
                  keystore.load(is, keyStorePassword.toCharArray());
                }
              } catch (Exception ex) {
                issues.add(context.createConfigIssue(Groups.RPC.name(), KEY_STORE_FILE, Errors.IPC_ORIG_10, ex.toString()));
              }
            }
          }
        }
      } else {
        issues.add(context.createConfigIssue(Groups.RPC.name(), KEY_STORE_FILE, Errors.IPC_ORIG_11));
      }
    }
  }

  File getKeyStoreFile(Stage.Context context) {
    return new File(context.getResourcesDirectory(), keyStoreFile);
  }

}
