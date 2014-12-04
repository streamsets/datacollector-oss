/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.config.Configuration;

import java.util.Iterator;

public interface Observer {

  public void init();

  public void destroy();

  public void configure(Configuration conf);

  public boolean isActive();

  public void observe(Batch batch);

}
