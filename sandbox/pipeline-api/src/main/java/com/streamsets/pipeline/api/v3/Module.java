/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3;

import com.codahale.metrics.MetricRegistry;

import java.util.List;

public interface Module<C extends Module.Context> {

  public interface Info {

    public String getInstance();

    public String getName();

    public String getVersion();

    public String getDescription();

  }

  public interface Context {

    public List<Info> getPipeline();

    public MetricRegistry getMetric();

  }

  public void init(Info info, C context, boolean previewMode);

  public Info getInfo();

  public void destroy();

}
