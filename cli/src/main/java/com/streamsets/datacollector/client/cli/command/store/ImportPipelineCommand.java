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
package com.streamsets.datacollector.client.cli.command.store;

import com.streamsets.datacollector.client.ApiClient;
import com.streamsets.datacollector.client.JSON;
import com.streamsets.datacollector.client.TypeRef;
import com.streamsets.datacollector.client.api.StoreApi;
import com.streamsets.datacollector.client.cli.command.BaseCommand;
import com.streamsets.datacollector.client.model.PipelineEnvelopeJson;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.io.File;

@Command(name = "import", description = "Import Pipeline Configuration & Rules")
public class ImportPipelineCommand extends BaseCommand {
  @Option(
    name = {"-n", "--name"},
    description = "Pipeline ID",
    required = true
  )
  public String pipelineId;

  @Option(
    name = {"-d", "--description"},
    description = "Pipeline Description",
    required = false
  )
  public String pipelineDescription;

  @Option(
    name = {"-f", "--file"},
    description = "Import file name",
    required = true
  )
  public String fileName;

  @Option(
      name = {"-o", "--overwrite"},
      description = "Overwrite if pipeline with name already exists",
      required = false
  )
  public boolean overwrite;

  @Override
  public void run() {
    try {
      ApiClient apiClient = getApiClient();
      StoreApi storeApi = new StoreApi(apiClient);
      if(fileName != null) {
        JSON json = apiClient.getJson();
        TypeRef returnType = new TypeRef<PipelineEnvelopeJson>() {};
        PipelineEnvelopeJson pipelineEnvelopeJson = json.deserialize(new File(fileName), returnType);
        storeApi.importPipeline(
            pipelineId,
            "0",
            overwrite,
            pipelineEnvelopeJson
        );
        System.out.println("Successfully imported from file '" + fileName + "' to pipeline - " + pipelineId );
      }
    } catch (Exception ex) {
      if(printStackTrace) {
        ex.printStackTrace();
      } else {
        System.out.println(ex.getMessage());
      }
    }
  }
}
