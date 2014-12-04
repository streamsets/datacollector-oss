/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

import com.codahale.metrics.MetricRegistry;

import java.util.List;

public interface Module<C extends Module.Context> {

  public interface Info {

    public String getName();

    public String getVersion();

    public String getDescription();

    public String getInstanceName();

  }

  public interface Context {

    public List<Info> getPipelineInfo();

    public MetricRegistry getMetrics();

  }

  public void init(Info info, C context);

  public Info getInfo();

  public void destroy();

}
