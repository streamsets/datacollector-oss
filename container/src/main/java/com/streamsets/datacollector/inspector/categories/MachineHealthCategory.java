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
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.ProcessUtil;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class MachineHealthCategory implements HealthCategory {
  private static final Logger LOG = LoggerFactory.getLogger(MachineHealthCategory.class);

  private static long MB = 1024 * 1024;

  @Override
  public String getName() {
    return "Machine";
  }

  @Override
  public HealthCategoryResult inspectHealth(HealthCategory.Context context) {
    HealthCategoryResult.Builder builder = new HealthCategoryResult.Builder(this);
    RuntimeInfo runtimeInfo = context.getRuntimeInfo();

    long unallocated = getUnallocatedSpace(runtimeInfo.getDataDir());
    builder.addHealthCheck("Data Dir Available Space", HealthCheck.Severity.higherIsBetter(unallocated, 1024 * MB, 100 * MB))
        .withValue(FileUtils.byteCountToDisplaySize(unallocated))
        .withDescription(Utils.format("Available space on filesystem hosting $DATA_DIR: {}", runtimeInfo.getDataDir()));

    unallocated = getUnallocatedSpace(runtimeInfo.getDataDir());
    builder.addHealthCheck("Runtime Dir Available Space", HealthCheck.Severity.higherIsBetter(unallocated, 1024 * MB, 100 * MB))
        .withValue(FileUtils.byteCountToDisplaySize(unallocated))
        .withDescription(Utils.format("Available space on filesystem hosting $RUNTIME_DIR: {}", runtimeInfo.getRuntimeDir()));

    unallocated = getUnallocatedSpace(runtimeInfo.getDataDir());
    builder.addHealthCheck("Log Dir Available Space", HealthCheck.Severity.higherIsBetter(unallocated, 1024 * MB, 100 * MB))
        .withValue(FileUtils.byteCountToDisplaySize(unallocated))
        .withDescription(Utils.format("Available space on filesystem hosting $LOG_DIR: {}", runtimeInfo.getLogDir()));

    // File descriptors
    HealthCheck.Severity ulimitSeverity = HealthCheck.Severity.GREEN;
    String ulimitDetails = null;
    List<String> ulimitCommand = ImmutableList.of("sh", "-c", "ulimit -n");
    ProcessUtil.Output ulimitOutput = ProcessUtil.executeCommandAndLoadOutput(ulimitCommand, 5);
    int ulimit = -1;
    try {
      String ulimitStdout = ulimitOutput.stdout.trim();
      if(ulimitStdout.isEmpty()) {
        ulimitSeverity = HealthCheck.Severity.RED;
        ulimitDetails = "Ulimit command failed: " + String.join(" ", ulimitCommand) + "\n" + ulimitOutput.stderr;
      } else {
        ulimit = Integer.parseInt(ulimitOutput.stdout.trim());
        ulimitSeverity = ulimit >= 32768 ? HealthCheck.Severity.GREEN : HealthCheck.Severity.RED;
      }
    } catch (Throwable e) {
      ulimitSeverity = HealthCheck.Severity.RED;
      ulimitDetails = "Ulimit command failed: " + String.join(" ", ulimitCommand) + "\n" + ExceptionUtils.getStackTrace(e);
    }
    builder.addHealthCheck("File Descriptors", ulimitSeverity)
        .withValue(ulimit != -1 ? ulimit : null)
        .withDescription("Number of file descriptors a single application can keep open at the same time.")
        .withDetails(ulimitDetails);

    // Number of processes running by sdc user
    String sdcUser = System.getProperty("user.name");
    ProcessUtil.Output userProcessesOutput = ProcessUtil.executeCommandAndLoadOutput(ImmutableList.of("ps", "-u", sdcUser, "-U", sdcUser), 5);
    int userProcesses = -1;
    if(!userProcessesOutput.success) {
      // On certain systems some of the options (-u or -U) aren't available, in that case we revert to use only ps
      LOG.debug("Can't use ps -U -u: {}", userProcessesOutput.stderr);
      userProcessesOutput = ProcessUtil.executeCommandAndLoadOutput(ImmutableList.of("ps"), 5);
    }

    if(userProcessesOutput.success && userProcessesOutput.stdout != null) {
      userProcesses = userProcessesOutput.stdout.split("\n").length;
    }
    builder.addHealthCheck("SDC User Processes", userProcesses == -1 ? HealthCheck.Severity.RED : HealthCheck.Severity.smallerIsBetter(userProcesses, 5, 10))
        .withValue(userProcesses == -1 ? null : userProcesses)
        .withDescription("Number of processes that are started by the user that started this JVM instance ({}).", sdcUser)
        .withDetails(userProcessesOutput);
    return builder.build();
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
