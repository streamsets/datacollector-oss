/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.restapi.configuration;

import com.streamsets.pipeline.main.BuildInfo;
import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

public class BuildInfoInjector implements Factory<BuildInfo> {
  public static final String BUILD_INFO = "build-info";
  private BuildInfo buildInfo;

  @Inject
  public BuildInfoInjector(HttpServletRequest request) {
    buildInfo = (BuildInfo) request.getServletContext().getAttribute(BUILD_INFO);
  }

  @Override
  public BuildInfo provide() {
    return buildInfo;
  }

  @Override
  public void dispose(BuildInfo buildInfo) {
  }

}
