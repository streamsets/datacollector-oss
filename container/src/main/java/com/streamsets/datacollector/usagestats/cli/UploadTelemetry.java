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
package com.streamsets.datacollector.usagestats.cli;

import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.usagestats.DCStatsCollectorTask;
import com.streamsets.datacollector.util.Configuration;
import dagger.ObjectGraph;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Command(name = "uploadTelemetry", description = "Upload Telemetry from an air-gapped deployment.")
public class UploadTelemetry implements Runnable {

  @Option(
      name = {"-f", "--file"},
      description = "Path to stats.json to be uploaded.",
      required = true
  )
  public String filePath;

  @Override
  public void run() {
    File file = new File(filePath);

    System.out.println(String.format("Uploading telemetry from %s", file.getAbsolutePath()));
    if (!file.exists() || !file.isFile()) {
      throw new RuntimeException(String.format("ERROR: file %s must be a valid stats.json", file.getAbsolutePath()));
    }

    ObjectGraph dagger = ObjectGraph.create(RuntimeModule.class);
    DCStatsCollectorTask statsTask = new DCStatsCollectorTask(
        dagger.get(BuildInfo.class),
        dagger.get(RuntimeInfo.class),
        dagger.get(Configuration.class),
        null,
        null,
        null
    );

    try {
      statsTask.reportStats(FileUtils.readFileToString(file, StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }
}
