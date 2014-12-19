/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.configuration;

import com.codahale.metrics.MetricRegistry;
import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

public class JvmMetricsInjector implements Factory<MetricRegistry> {

  public static final String JVM_METRICS = "jvm-metrics";
  private MetricRegistry metrics;

  @Inject
  public JvmMetricsInjector(HttpServletRequest request) {
    metrics = (MetricRegistry) request.getServletContext().getAttribute(JVM_METRICS);
  }

  @Override
  public MetricRegistry provide() {
    return metrics;
  }

  @Override
  public void dispose(MetricRegistry metrics) {
  }

}
