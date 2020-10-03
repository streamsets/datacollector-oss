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
package com.streamsets.datacollector.inspector.inspectors;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.streamsets.datacollector.inspector.HealthInspector;
import com.streamsets.datacollector.inspector.model.HealthInspectorResult;
import com.streamsets.datacollector.inspector.model.HealthInspectorEntry;
import com.streamsets.datacollector.util.ProcessUtil;

import java.nio.charset.Charset;
import java.util.List;


public class NetworkInspector implements HealthInspector {
  @Override
  public String getName() {
    return "Networking";
  }

  @Override
  public HealthInspectorResult inspectHealth(Context context) {
    HealthInspectorResult.Builder builder = new HealthInspectorResult.Builder(this);

    runCommand(builder, "Ping", "Ping to www.google.com", ImmutableList.of("ping", "-v", "-t", "2", "www.google.com"));
    runCommand(builder, "Traceroute", "Traceroute to www.google.com", ImmutableList.of("traceroute", "-w", "1", "-q", "1", "-v", "www.google.com"));

    return builder.build();
  }

  private void runCommand(
      HealthInspectorResult.Builder builder,
      String name,
      String description,
      List<String> command
  ) {
    StringBuilder stdoutBuilder = new StringBuilder();
    StringBuilder stderrBuilder = new StringBuilder();

    // Ping
    boolean success = ProcessUtil.executeCommand(
        command,
        5,
        (out, err) -> {
          stdoutBuilder.append(Files.toString(out.toFile(), Charset.defaultCharset()));
          stderrBuilder.append(Files.toString(err.toFile(), Charset.defaultCharset()));
        }
    );
    builder.addEntry(name, success ? HealthInspectorEntry.Severity.GREEN : HealthInspectorEntry.Severity.RED)
        .withDescription(description)
        .withDetails("stdout:\n" + stdoutBuilder.toString() + "\n\nstderr:\n" + stderrBuilder);
  }
}
