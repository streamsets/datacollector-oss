/**
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.bundles.content;

import com.streamsets.datacollector.bundles.BundleContentGenerator;
import com.streamsets.datacollector.bundles.BundleContentGeneratorDef;
import com.streamsets.datacollector.bundles.BundleContext;
import com.streamsets.datacollector.bundles.BundleWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

@BundleContentGeneratorDef(
  name = "Logs",
  description = "Most recent logs.",
  version = 1,
  enabledByDefault = true
)
public class LogContentGenerator implements BundleContentGenerator {
  @Override
  public void generateContent(BundleContext context, BundleWriter writer) throws IOException {
    // This is very naive implementation that simply copies all files from log directory.
    // Production ready implementation will need to add some sort of max limit for the logs
    // and read only the "last" logs.
    File logDir = new File(context.getRuntimeInfo().getLogDir());
    if(!logDir.exists() || !logDir.isDirectory()) {
      return;
    }

    for(File file : logDir.listFiles()) {
      if(file.isFile()) {
        writer.write("", Paths.get(file.toURI()));
      }
    }
  }
}
