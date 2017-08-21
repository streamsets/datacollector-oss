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
package com.streamsets.datacollector.restapi.configuration;

import com.streamsets.datacollector.execution.Manager;

import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

public class StandAndClusterManagerInjector implements Factory<Manager> {

  public static final String PIPELINE_MANAGER_MGR = "pipeline-manager";

  private Manager manager;

  @Inject
  public StandAndClusterManagerInjector(HttpServletRequest request) {
    manager = (Manager) request.getServletContext().getAttribute(PIPELINE_MANAGER_MGR);
  }

  @Override
  public Manager provide() {
    return manager;
  }

  @Override
  public void dispose(Manager manager) {
  }
}
