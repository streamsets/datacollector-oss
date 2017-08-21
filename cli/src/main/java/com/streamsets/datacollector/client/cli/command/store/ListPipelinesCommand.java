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
package com.streamsets.datacollector.client.cli.command.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.streamsets.datacollector.client.api.StoreApi;
import com.streamsets.datacollector.client.cli.command.BaseCommand;
import com.streamsets.datacollector.client.model.Order;
import com.streamsets.datacollector.client.model.PipelineOrderByFields;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "list", description = "List all available pipelines info")
public class ListPipelinesCommand extends BaseCommand {

  @Option(
      name = {"--filterText"},
      description = "Filter Text",
      required = false
  )
  public String filterText;

  @Option(
      name = {"--label"},
      description = "Pipeline Label",
      required = false
  )
  public String label;

  @Option(
      name = {"--offset"},
      description = "offset",
      required = false
  )
  public int offset = 0;

  @Option(
      name = {"--length"},
      description = "length",
      required = false
  )
  public int length = -1;

  @Option(
      name = {"--orderBy"},
      description = "Order the results by column. Supported values - NAME/LAST_MODIFIED/CREATED/STATUS",
      required = false
  )
  public String orderBy = "NAME";

  @Option(
      name = {"--order"},
      description = "Sort order. Supported values - ASC/DESC",
      required = false
  )
  public String order = "ASC";

  @Option(
      name = {"--includeStatus"},
      description = "includeStatus",
      required = false
  )
  public boolean includeStatus;

  @Override
  public void run() {
    try {
      StoreApi storeApi = new StoreApi(getApiClient());
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

      System.out.println(mapper.writeValueAsString(
          storeApi.getPipelines(
              filterText,
              label,
              offset,
              length,
              PipelineOrderByFields.valueOf(orderBy),
              Order.valueOf(order),
              includeStatus
          )
      ));
    } catch (Exception ex) {
      if(printStackTrace) {
        ex.printStackTrace();
      } else {
        System.out.println(ex.getMessage());
      }
    }
  }
}
