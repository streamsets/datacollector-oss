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
import com.streamsets.datacollector.inspector.HealthInspector;
import com.streamsets.datacollector.inspector.model.HealthInspectorResult;
import com.streamsets.datacollector.inspector.model.HealthInspectorEntry;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.ProcessUtil;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class MachineInspector implements HealthInspector {

  private static long MB = 1024 * 1024;

  @Override
  public String getName() {
    return "Machine";
  }

  @Override
  public HealthInspectorResult inspectHealth(HealthInspector.Context context) {
    HealthInspectorResult.Builder builder = new HealthInspectorResult.Builder(this);
    RuntimeInfo runtimeInfo = context.getRuntimeInfo();

    long unallocated = getUnallocatedSpace(runtimeInfo.getDataDir());
    builder.addEntry("Data Dir Available Space", HealthInspectorEntry.Severity.higherIsBetter(unallocated, 1024 * MB, 100 * MB))
        .withValue(FileUtils.byteCountToDisplaySize(unallocated))
        .withDescription(Utils.format("Available space on filesystem hosting $DATA_DIR: {}", runtimeInfo.getDataDir()));

    unallocated = getUnallocatedSpace(runtimeInfo.getDataDir());
    builder.addEntry("Runtime Dir Available Space", HealthInspectorEntry.Severity.higherIsBetter(unallocated, 1024 * MB, 100 * MB))
        .withValue(FileUtils.byteCountToDisplaySize(unallocated))
        .withDescription(Utils.format("Available space on filesystem hosting $RUNTIME_DIR: {}", runtimeInfo.getRuntimeDir()));

    unallocated = getUnallocatedSpace(runtimeInfo.getDataDir());
    builder.addEntry("Log Dir Available Space", HealthInspectorEntry.Severity.higherIsBetter(unallocated, 1024 * MB, 100 * MB))
        .withValue(FileUtils.byteCountToDisplaySize(unallocated))
        .withDescription(Utils.format("Available space on filesystem hosting $LOG_DIR: {}", runtimeInfo.getLogDir()));

    // File descriptors
    HealthInspectorEntry.Severity ulimitSeverity = HealthInspectorEntry.Severity.GREEN;
    String ulimitDetails = null;
    String ulimitOutput = stdoutForCommand(ImmutableList.of("ulimit", "-n"));
    int ulimit = -1;
    try {
      ulimit = Integer.parseInt(ulimitOutput.trim());

      ulimitSeverity = ulimit >= 32768 ? HealthInspectorEntry.Severity.GREEN : HealthInspectorEntry.Severity.RED;
    } catch (Throwable e) {
      ulimitSeverity = HealthInspectorEntry.Severity.RED;
      ulimitDetails = ExceptionUtils.getStackTrace(e);
    }
    builder.addEntry("File Descriptors", ulimitSeverity)
        .withValue(ulimit != -1 ? ulimit : null)
        .withDescription("Number of file descriptors a single application can keep open at the same time.")
        .withDetails(ulimitDetails);

    return builder.build();
  }

  private String stdoutForCommand(List<String> command) {
    StringBuilder stdoutBuilder = new StringBuilder();

    // Ping
    boolean success = ProcessUtil.executeCommand(
        command,
        5,
        (out, err) -> {
          stdoutBuilder.append(com.google.common.io.Files.toString(out.toFile(), Charset.defaultCharset()));
        }
    );

    return success ? stdoutBuilder.toString() : null;
  }

  public long getUnallocatedSpace(String directory) {
    try {
      Path path = Paths.get(directory);
      FileStore store = Files.getFileStore(path.getRoot());
      return store.getUnallocatedSpace();
    } catch (IOException e) {
      return -1;
    }
  }
}
