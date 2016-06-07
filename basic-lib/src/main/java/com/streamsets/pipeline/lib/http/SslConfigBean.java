/*
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.http;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.origin.http.Errors;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class SslConfigBean {
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Path to Trust Store",
      displayPosition = 10,
      group = "#0"
  )
  public String trustStorePath = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Password",
      displayPosition = 20,
      group = "#0"
  )
  public String trustStorePassword = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Path to Keystore Store",
      displayPosition = 30,
      group = "#0"
  )
  public String keyStorePath = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Password",
      displayPosition = 40,
      group = "#0"
  )
  public String keyStorePassword = "";

  public void init(Source.Context context, String groupName, String prefix, List<Stage.ConfigIssue> issues) {
    if (!keyStorePath.isEmpty() && Files.notExists(Paths.get(keyStorePath))) {
      issues.add(context.createConfigIssue(
          groupName,
          prefix + keyStorePath,
          Errors.HTTP_04,
          keyStorePath
      ));

      if (keyStorePassword.isEmpty()) {
        issues.add(context.createConfigIssue(
            groupName,
            prefix + keyStorePassword,
            Errors.HTTP_04
        ));
      }
    }

    if (!trustStorePath.isEmpty() && Files.notExists(Paths.get(trustStorePath))) {
      issues.add(context.createConfigIssue(
          groupName,
          prefix + trustStorePath,
          Errors.HTTP_05,
          trustStorePath
      ));

      if (trustStorePassword.isEmpty()) {
        issues.add(context.createConfigIssue(
            groupName,
            "conf.sslConfig.trustStorePassword",
            Errors.HTTP_05
        ));
      }
    }
  }
}
