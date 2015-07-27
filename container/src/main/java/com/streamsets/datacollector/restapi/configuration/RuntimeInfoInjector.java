/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.configuration;

import com.streamsets.datacollector.main.RuntimeInfo;

import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

public class RuntimeInfoInjector implements Factory<RuntimeInfo> {
  public static final String RUNTIME_INFO = "runtime-info";
  private RuntimeInfo runtimeInfo;

  @Inject
  public RuntimeInfoInjector(HttpServletRequest request) {
    runtimeInfo = (RuntimeInfo) request.getServletContext().getAttribute(RUNTIME_INFO);
  }

  @Override
  public RuntimeInfo provide() {
    return runtimeInfo;
  }

  @Override
  public void dispose(RuntimeInfo runtimeInfo) {
  }

}
