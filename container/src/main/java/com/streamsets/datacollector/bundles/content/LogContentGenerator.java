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
package com.streamsets.datacollector.bundles.content;

import com.streamsets.datacollector.bundles.BundleContentGenerator;
import com.streamsets.datacollector.bundles.BundleContentGeneratorDef;
import com.streamsets.datacollector.bundles.BundleContext;
import com.streamsets.datacollector.bundles.BundleWriter;
import com.streamsets.datacollector.bundles.Constants;
import com.streamsets.datacollector.log.LogUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.io.FilenameFilter;

@BundleContentGeneratorDef(
  name = "Logs",
  description = "Most recent logs.",
  version = 1,
  enabledByDefault = true,
  // We want logs generator to run last, so that logs contains any possible exceptions about generating this bundle
  order = Integer.MAX_VALUE
)
public class LogContentGenerator implements BundleContentGenerator {
  @Override
  public void generateContent(BundleContext context, BundleWriter writer) throws IOException {
    // Sort the log files in descending manner (e.g. get the most up to date logs first)
    List<File> logFiles = Arrays.stream(LogUtils.getLogFiles(context.getRuntimeInfo()))
      .sorted((f1, f2) -> f1.lastModified() < f2.lastModified() ? 1 : -1)
      .collect(Collectors.toList());

    // Write as many log files as for which we have actual space
    long availableSpace = context.getConfiguration().get(Constants.LOG_MAX_SIZE, Constants.DEFAULT_LOG_MAX_SIZE);
    for(File logFile : logFiles) {
      // As long as we have not exhausted quota for logs
      if(availableSpace <= 0) {
        break;
      }

      writer.write("", logFile.toPath(), logFile.length() - availableSpace);
      availableSpace -= logFile.length();
    }

    // GC log
    long availableGcSpace = context.getConfiguration().get(Constants.LOG_GC_MAX_SIZE, Constants.DEFAULT_LOG_GC_MAX_SIZE);
    Path gcLog = Paths.get(context.getRuntimeInfo().getLogDir(), "gc.log");
    if(Files.exists(gcLog)) {
      writer.write("", gcLog, Files.size(gcLog) - availableGcSpace);
    }
    //JVM crash File

    long availableHsSpace = context.getConfiguration().get(Constants.LOG_HS_MAX_SIZE, Constants.DEFAULT_LOG_HS_MAX_SIZE);
    File dir = new File(context.getRuntimeInfo().getLogDir());
    File[] hsFiles = dir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.startsWith("hs_err");
      }
    });

    if (hsFiles != null) {
      for(File hsFile : hsFiles) {
        // As long as we have not exhausted quota for logs
        if(availableHsSpace <= 0) {
          break;
        }
        writer.write("", hsFile.toPath(), hsFile.length() - availableHsSpace);
        availableHsSpace -= hsFile.length();

      }
    }

  }
}

