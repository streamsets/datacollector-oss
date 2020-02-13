/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.testing.pipeline.stage;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUpgraderContext implements StageUpgrader.Context {

  private final String library;
  private final String stageName;
  private final String stageInstanceName;
  private final int fromVersion;
  private final int toVersion;
  private final Map<Class, List<Config>> registeredServices;

  public TestUpgraderContext(
      String library,
      String stageName,
      String stageInstanceName,
      int fromVersion,
      int toVersion
  ) {
    this.library = library;
    this.stageName = stageName;
    this.stageInstanceName = stageInstanceName;
    this.fromVersion = fromVersion;
    this.toVersion = toVersion;
    this.registeredServices = new HashMap<>();
  }

  @Override
  public String getLibrary() {
    return library;
  }

  @Override
  public String getStageName() {
    return stageName;
  }

  @Override
  public String getStageInstance() {
    return stageInstanceName;
  }

  @Override
  public int getFromVersion() {
    return fromVersion;
  }

  @Override
  public int getToVersion() {
    return toVersion;
  }

  @Override
  public void registerService(Class service, List<Config> configs) {
    registeredServices.put(service, configs);
  }

  public Map<Class, List<Config>> getRegisteredServices() {
    return registeredServices;
  }
}
