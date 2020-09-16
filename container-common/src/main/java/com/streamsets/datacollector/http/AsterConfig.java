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
package com.streamsets.datacollector.http;

import com.streamsets.datacollector.util.Configuration;

/**
 * Bean than encapsulates the configuration to create an Aster authenticator.
 * <p/>
 * As the Aster authenticator is created in another classloader this bean simplifies
 * passing its configuration.
 */
public class AsterConfig {
  private final String engineType;
  private final String engineVersion;
  private final String engineId;
  private final Configuration engineConf;
  private final String dataDir;

  /**
   * Constructor.
   */
  public AsterConfig(
      String engineType,
      String engineVersion,
      String engineId,
      Configuration engineConf,
      String dataDir
  ) {
    this.engineType = engineType;
    this.engineVersion = engineVersion;
    this.engineId = engineId;
    this.engineConf = engineConf;
    this.dataDir = dataDir;
  }

  /**
   * Returns the engine type, {@code DC} or {@code TF}.
   * @return
   */
  public String getEngineType() {
    return engineType;
  }

  /**
   * Returns the engine version.
   * @return
   */
  public String getEngineVersion() {
    return engineVersion;
  }

  /**
   * Returns the engine ID.
   */
  public String getEngineId() {
    return engineId;
  }

  /**
   * Returns the engine configuration.
   */
  public Configuration getEngineConf() {
    return engineConf;
  }

  /**
   * Returns the engine data directory (it will be used to store the Aster tokens).
   */
  public String getDataDir() {
    return dataDir;
  }
}
