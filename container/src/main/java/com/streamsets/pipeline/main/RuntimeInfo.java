/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.main;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;

import java.util.List;

public class RuntimeInfo {
  private final List<? extends ClassLoader> stageLibraryClassLoaders;

  public RuntimeInfo(List<? extends ClassLoader> stageLibraryClassLoaders) {
    this.stageLibraryClassLoaders = ImmutableList.copyOf(stageLibraryClassLoaders);
  }

  public String getRuntimeDir() {
    return System.getProperty("user.dir");
  }

  public String getStaticWebDir() {
    return System.getProperty("pipeline.static-web.dir", getRuntimeDir() + "/static-web");
  }

  public String getConfigDir() {
    return System.getProperty("pipeline.conf.dir", getRuntimeDir() + "/etc");
  }

  public String getLogDir() {
    return System.getProperty("pipeline.log.dir", getRuntimeDir() + "/log");
  }

  public String getDataDir() {
    return System.getProperty("pipeline.data.dir", getRuntimeDir() + "/var");
  }

  public List<? extends ClassLoader> getStageLibraryClassLoaders() {
    return stageLibraryClassLoaders;
  }

  public void log(Logger log) {
    log.info("Runtime info:");
    log.info("  Java version : {}", System.getProperty("java.runtime.version"));
    log.info("  Runtime dir  : {}", getRuntimeDir());
    log.info("  Config dir   : {}", getConfigDir());
    log.info("  Data dir     : {}", getDataDir());
    log.info("  Log dir      : {}", getLogDir());
  }

}
