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
package com.streamsets.datacollector.client.cli.command.system;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Strings;
import com.streamsets.datacollector.client.api.SystemApi;
import com.streamsets.datacollector.client.cli.command.BaseCommand;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

@Command(name = "stats", description = "Returns Data Collector Stats")
public class StatsCommand extends BaseCommand {

  @Option(
      name = {"-f", "--file"},
      description = "Write results to given file.",
      required = false
  )
  public String file;

  @Override
  public void run() {
    try {
      SystemApi systemApi = new SystemApi(getApiClient());
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
      String output = mapper.writeValueAsString(systemApi.getStats());
      if (Strings.isNullOrEmpty(file)) {
        System.out.println(output);
      } else {
        System.out.println("Writing output to file: " + file);
        Files.write(Paths.get(file), output.getBytes(StandardCharsets.UTF_8));
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
