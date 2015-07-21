/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.restapi.configuration;

import com.streamsets.pipeline.util.Configuration;
import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

public class ConfigurationInjector implements Factory<Configuration> {
  public static final String CONFIGURATION = "configuration";

  private Configuration configuration;

  @Inject
  public ConfigurationInjector(HttpServletRequest request) {
    configuration = (Configuration) request.getServletContext().getAttribute(CONFIGURATION);
  }

  @Override
  public Configuration provide() {
    return configuration;
  }

  @Override
  public void dispose(Configuration configuration) {
  }

}
