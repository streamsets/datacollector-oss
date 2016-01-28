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
package com.streamsets.datacollector.client.cli.command;

import com.streamsets.datacollector.client.ApiClient;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

public class BaseCommand implements Runnable {
  @Option(
    type = OptionType.GLOBAL,
    name = {"-U", "--url"},
    description = "Data Collector URL",
    required = true
  )
  public String sdcURL;

  @Option(
    type = OptionType.GLOBAL,
    name = {"-u", "--user"},
    description = "Data Collector User",
    required = false
  )
  public String sdcUser;

  @Option(
    type = OptionType.GLOBAL,
    name = {"-p", "--password"},
    description = "Data Collector password",
    required = false
  )
  public String sdcPassword;

  @Option(
    type = OptionType.GLOBAL,
    name = {"-a", "--auth-type"},
    description = "Data Collector Authentication Type",
    allowedValues = {"none", "basic", "digest", "form"},
    required = false
  )
  public String sdcAuthType;

  @Option(
    name = {"--stack"},
    description = "Print a stack trace when exiting with a warning or fatal error.",
    required = false
  )
  public boolean printStackTrace;

  @Override
  public void run() {
    System.out.println(getClass().getSimpleName());
  }

  public ApiClient getApiClient() {
    if(sdcAuthType == null) {
      sdcAuthType = "form";
    }

    if(sdcUser == null) {
      sdcUser = "admin";
    }

    if(sdcPassword == null) {
      sdcPassword = "admin";
    }

    ApiClient apiClient = new ApiClient(sdcAuthType);
    apiClient.setBasePath(sdcURL + "/rest");
    apiClient.setUsername(sdcUser);
    apiClient.setPassword(sdcPassword);
    return  apiClient;
  }
}
