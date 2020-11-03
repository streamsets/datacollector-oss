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
package com.streamsets.datacollector.inspector;

import com.streamsets.datacollector.inspector.model.HealthCategoryResult;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;

/**
 * Individual inspectors that can run various health checks on the system.
 */
public interface HealthCategory {

  interface Context {
    /**
     * Return configuration of the data collector.
     */
    Configuration getConfiguration();

    /**
     * Runtime info structure for this JVM.
     */
    RuntimeInfo getRuntimeInfo();
  }

  /**
   * Human readable name that can be displayed in the UI on top of all the individual entries in report.
   */
  String getName();

  /**
   * Perform the actual health inspection for this checker.
   */
  HealthCategoryResult inspectHealth(Context context);

}
