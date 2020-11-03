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

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.inspector.categories.ConfigurationHealthCategory;
import com.streamsets.datacollector.inspector.categories.MachineHealthCategory;
import com.streamsets.datacollector.inspector.categories.NetworkHealthCategory;
import com.streamsets.datacollector.inspector.categories.JvmInstanceHealthCategory;
import com.streamsets.datacollector.inspector.model.HealthCategoryResult;
import com.streamsets.datacollector.inspector.model.HealthReport;
import com.streamsets.datacollector.inspector.model.HeathCategoryInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class HealthInspectorManager implements HealthCategory.Context {
  private static final Logger LOG = LoggerFactory.getLogger(HealthInspectorManager.class);

  /**
   * List of registered inspectors.
   */
  private static final List<HealthCategory> CATEGORIES = ImmutableList.of(
      new ConfigurationHealthCategory(),
      new JvmInstanceHealthCategory(),
      new MachineHealthCategory(),
      new NetworkHealthCategory()
  );

  private final Configuration configuration;
  private final RuntimeInfo runtimeInfo;

  public HealthInspectorManager(
      Configuration configuration,
      RuntimeInfo runtimeInfo
  ) {
    this.configuration = configuration;
    this.runtimeInfo = runtimeInfo;
  }

  /**
   * Return list of available inspectors.
   */
  public List<HeathCategoryInfo> availableInspectors() {
    return CATEGORIES.stream().map(HeathCategoryInfo::new).collect(Collectors.toList());
  }

  /**
   * Actually run the health checks and return their output.
   *
   * @param categories If empty, run all categories, otherwise run only selected ones
   */
  public HealthReport inspectHealth(List<String> categories) {
    LOG.info("Running Health Inspector with following categories: {}", String.join(",", categories));
    List<HealthCategoryResult> checks = new LinkedList<>();
    long startTime = System.currentTimeMillis();

    for(HealthCategory category : CATEGORIES) {
      if(categories.isEmpty() || categories.contains(category.getClass().getSimpleName())) {
        checks.add(category.inspectHealth(this));
      }
    }

    return new HealthReport(
        LocalDateTime.now().toString(),
        System.currentTimeMillis() - startTime,
        checks
    );
  }

  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  @Override
  public RuntimeInfo getRuntimeInfo() {
    return runtimeInfo;
  }
}
