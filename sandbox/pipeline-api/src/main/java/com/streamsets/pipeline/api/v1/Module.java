/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v1;

import com.codahale.metrics.MetricRegistry;

import java.util.List;

public interface Module {

  public interface Info {

    public String getInstance();

    public String getName();

    public String getVersion();

    public String getDescription();

  }

  interface Context {

    public List<String> getPipeline();

    public MetricRegistry getMetric();

    public Iterable<String> getAttributeNames();

    public <T> T getAttribute(String name, Class<T> klass);

    public <T> void setAttribute(String name, T obj);

    public void removeAttribute(String name);

  }

  public void init(Info info, Context context);

  public void destroy();

}
