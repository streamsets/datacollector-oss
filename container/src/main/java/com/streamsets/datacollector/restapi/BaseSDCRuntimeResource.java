/*
 * Copyright 2016 StreamSets Inc.
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

package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.log.LogUtils;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;

import javax.inject.Inject;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

public class BaseSDCRuntimeResource {

  private BuildInfo buildInfo;
  private RuntimeInfo runtimeInfo;

  @Inject
  public BaseSDCRuntimeResource(BuildInfo buildInfo, RuntimeInfo runtimeInfo) {
    this.buildInfo = buildInfo;
    this.runtimeInfo = runtimeInfo;
  }

  protected File[] determineAndGetAllLogFiles() throws IOException {
    return getAllLogFilesInDirectory(LogUtils.getLogFile(runtimeInfo));
  }

  protected File[] getAllLogFilesInDirectory(String startingLogFile) throws IOException {
    File log = new File(startingLogFile);
    File logDir = log.getParentFile();
    final String logName = log.getName();
    return logDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(logName);
      }
    });
  }
}
