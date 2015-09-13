/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.client.cli.command.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.streamsets.datacollector.client.api.StoreApi;
import com.streamsets.datacollector.client.cli.command.BaseCommand;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "get-config", description = "Get Pipeline Configuration")
public class GetPipelineConfigCommand extends BaseCommand {
  @Option(
    name = {"-n", "--name"},
    description = "Pipeline Name",
    required = true
  )
  public String pipelineName;

  @Option(
    name = {"-r", "--revision"},
    description = "Pipeline Revision",
    required = false
  )
  public String pipelineRev;

  @Option(
    name = "--get",
    description = "Get Pipeline Configuration or Info",
    allowedValues = {"info", "pipeline"},
    required = false
  )
  public String get;

  public void run() {
    if(pipelineRev == null) {
      pipelineRev = "0";
    }

    if(get == null) {
      get = "pipeline";
    }
    StoreApi storeApi = new StoreApi(getApiClient());
    try {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
      System.out.println(mapper.writeValueAsString(storeApi.getPipelineInfo(pipelineName, pipelineRev, get, false)));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
