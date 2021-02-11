/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.inspector.categories;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.inspector.HealthCategory;
import com.streamsets.datacollector.inspector.model.HealthCategoryResult;
import com.streamsets.datacollector.inspector.model.HealthCheck;
import com.streamsets.datacollector.util.ProcessUtil;

import java.util.List;


public class NetworkHealthCategory implements HealthCategory {

  private static final String HOST_KEY = "health_inspector.network.host";
  private static final String HOST_DEFAULT = "www.streamsets.com";

  @Override
  public String getName() {
    return "Networking";
  }

  @Override
  public HealthCategoryResult inspectHealth(Context context) {
    HealthCategoryResult.Builder builder = new HealthCategoryResult.Builder(this);

    String host = context.getConfiguration().get(HOST_KEY, HOST_DEFAULT);

    runCommand(builder, "Ping", "Ping to " + host, ImmutableList.of("ping", "-v", "-t", "2", host));
    runCommand(builder, "Traceroute", "Traceroute to " + host, ImmutableList.of("traceroute", "-w", "1", "-q", "1", host));

    return builder.build();
  }

  private void runCommand(
      HealthCategoryResult.Builder builder,
      String name,
      String description,
      List<String> command
  ) {
    ProcessUtil.Output output = ProcessUtil.executeCommandAndLoadOutput(command, 5);
    builder.addHealthCheck(name, output.success ? HealthCheck.Severity.GREEN : HealthCheck.Severity.RED)
        .withDescription(description)
        .withDetails(output);
  }
}
