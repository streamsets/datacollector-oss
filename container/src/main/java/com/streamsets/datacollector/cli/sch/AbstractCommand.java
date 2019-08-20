/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.cli.sch;

import com.streamsets.datacollector.main.MainStandalonePipelineManagerModule;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.util.Configuration;
import dagger.ObjectGraph;
import io.airlift.airline.Option;

abstract public class AbstractCommand implements Runnable {

  @Option(
    name = {"-u", "--user"},
    description = "Username",
    required = true
  )
  private String userID;
  protected String getUserID() {
    return userID;
  }

  @Option(
    name = {"-p", "--password"},
    description = "Password",
    required = true
  )
  private String userPassword;
  protected String getUserPassword() {
    return userPassword;
  }

  @Option(
    name = {"--token-file-path"},
    description = "Path to file where SCH app token should be stored",
    required = false
  )
  private String tokenFilePath;
  protected String getTokenFilePath() {
    return tokenFilePath;
  }

  @Option(
    name = {"--skip-config-update"},
    description = "Do not update dpm.properties configuration file.",
    required = false
  )
  private boolean skipConfigUpdate;
  protected boolean isSkipConfigUpdate() {
    return skipConfigUpdate;
  }

  @Option(
      name = {"--product"},
      description = "Product name (ex: sdc, transformer, etc.)",
      allowedValues = {"sdc", "transformer"}
  )
  private String productName = "sdc";

  protected String getProductName() {
    return productName;
  }

  protected String getOrganization() {
    String []parts = userID.split("@");
    if(parts.length != 2) {
      throw new RuntimeException("Expected username in format name@organization.");
    }

    return parts[1];
  }

  private RuntimeInfo runtimeInfo;
  protected RuntimeInfo getRuntimeInfo() {
    return runtimeInfo;
  }

  private Configuration configuration;
  protected Configuration getConfiguration() {
    return configuration;
  }

  @Override
  public void run() {
    RuntimeModule.setProductName(getProductName());
    // for now, product name and property prefix are the same, so just set it here instead of requiring new arg
    RuntimeModule.setPropertyPrefix(getProductName());
    ObjectGraph dagger = ObjectGraph.create(MainStandalonePipelineManagerModule.class);

    this.runtimeInfo = dagger.get(RuntimeInfo.class);
    this.configuration = dagger.get(Configuration.class);
    try {
      executeAction();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void executeAction() throws Exception;
}
