/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.restapi.configuration;

import com.streamsets.dc.execution.Manager;
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
