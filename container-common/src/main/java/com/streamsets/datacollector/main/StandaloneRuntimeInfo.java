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
package com.streamsets.datacollector.main;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.uuid.Generators;
import com.google.common.io.Files;
import com.streamsets.datacollector.security.usermgnt.TrxUsersManager;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;

public class StandaloneRuntimeInfo extends RuntimeInfo {
  private String id;
  private File baseDir;

  public StandaloneRuntimeInfo(
      String productName,
      String propertyPrefix,
      MetricRegistry metrics,
      List<? extends ClassLoader> stageLibraryClassLoaders
  ) {
    super(productName, propertyPrefix, metrics, stageLibraryClassLoaders);
  }

  public StandaloneRuntimeInfo(
      String productName,
      String propertyPrefix,
      MetricRegistry metrics,
      List<? extends ClassLoader> stageLibraryClassLoaders,
      File baseDir
  ) {
    super(productName, propertyPrefix, metrics, stageLibraryClassLoaders);
    this.baseDir = baseDir;
  }

  @Override
  public void init() {
    this.id = getSdcId(getDataDir());
    // inject SDC ID into the API sdc:id EL function
    Utils.setSdcIdCallable(new Callable<String>() {
      @Override
      public String call() throws Exception {
        return StandaloneRuntimeInfo.this.id;
      }
    });

    // to force a completion or rollback of an incomplete user management operation
    File usersFile = new File(getConfigDir(), "/form-realm.properties");
    File[] files = usersFile.getParentFile().listFiles((dir, name) -> name.startsWith("form-realm.properties"));
    if (files != null && files.length > 0) {
      try {
        new TrxUsersManager(usersFile);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Override
  public String getRuntimeDir() {
    if (baseDir != null) {
      return baseDir.getAbsolutePath();
    }
    if (Boolean.getBoolean(propertyPrefix + ".testing-mode")) {
      return System.getProperty("test.data.dir") + "/runtime-" + getRandomUUID();
    }
    return System.getProperty("user.dir");

  }

  private String getSdcId(String dir) {
    File dataDir = new File(dir);
    if (!dataDir.exists()) {
      if (!dataDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create data directory '{}'", dataDir));
      }
    }
    File idFile = new File(dataDir, "sdc.id");
    if (!idFile.exists()) {
      try {
        Files.write(Generators.timeBasedGenerator().generate().toString(), idFile, StandardCharsets.UTF_8);
      } catch (IOException ex) {
        throw new RuntimeException(Utils.format("Could not create SDC ID file '{}': {}", idFile, ex.toString()), ex);
      }
    }
    try {
      return Files.readFirstLine(idFile, StandardCharsets.UTF_8).trim();
    } catch (IOException ex) {
      throw new RuntimeException(Utils.format("Could not read SDC ID file '{}': {}", idFile, ex.toString()), ex);
    }
  }

  @Override
  public String getId() {
    return id;
  }

  // Return id
  @Override
  public String getMasterSDCId() {
    return null;
  }

  @Override
  public boolean isClusterSlave() {
    return false;
  }

  public void setBaseDir(File baseDir) {
    this.baseDir = baseDir;
  }
}
