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
import com.streamsets.datacollector.inspector.inspectors.ConfigurationInspector;
import com.streamsets.datacollector.inspector.inspectors.MachineInspector;
import com.streamsets.datacollector.inspector.inspectors.NetworkInspector;
import com.streamsets.datacollector.inspector.inspectors.SdcServerInstanceInspector;
import com.streamsets.datacollector.inspector.model.HealthInspectorResult;
import com.streamsets.datacollector.inspector.model.HealthInspectorReport;
import com.streamsets.datacollector.inspector.model.HealthInspectorsInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class HealthInspectorManager implements HealthInspector.Context {
  private static final Logger LOG = LoggerFactory.getLogger(HealthInspectorManager.class);

  /**
   * List of registered inspectors.
   */
  private static final List<HealthInspector> INSPECTORS = ImmutableList.of(
      new ConfigurationInspector(),
      new SdcServerInstanceInspector(),
      new MachineInspector(),
      new NetworkInspector()
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
  public List<HealthInspectorsInfo> availableInspectors() {
    return INSPECTORS.stream().map(HealthInspectorsInfo::new).collect(Collectors.toList());
  }

  /**
   * Actually run the health checks and return their output.
   *
   * @param inspectors If empty, run all inspectors, otherwise run only selected ones
   */
  public HealthInspectorReport inspectHealth(List<String> inspectors) {
    LOG.info("Running Health Inspector with following inspectors: {}", String.join(",", inspectors));
    List<HealthInspectorResult> checks = new LinkedList<>();

    for(HealthInspector checker : INSPECTORS) {
      if(inspectors.isEmpty() || inspectors.contains(checker.getClass().getSimpleName())) {
        checks.add(checker.inspectHealth(this));
      }
    }

    return new HealthInspectorReport(checks);
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
