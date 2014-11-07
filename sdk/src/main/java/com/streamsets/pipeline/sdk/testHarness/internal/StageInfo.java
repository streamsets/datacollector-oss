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
package com.streamsets.pipeline.sdk.testharness.internal;

import com.streamsets.pipeline.api.Stage;

public class StageInfo implements Stage.Info {

  private final String name;
  private final String version;
  private final String instanceName;

  public StageInfo(String name, String version, String instanceName) {
    this.name = name;
    this.version = version;
    this.instanceName = instanceName;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getVersion() {
    return version;
  }

  @Override
  public String getInstanceName() {
    return instanceName;
  }
}
