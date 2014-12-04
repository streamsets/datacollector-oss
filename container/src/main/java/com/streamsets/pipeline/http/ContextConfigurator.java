/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.http;

import org.eclipse.jetty.servlet.ServletContextHandler;

public abstract class ContextConfigurator {

  public abstract void init(ServletContextHandler context);

  public void start() {
  }

  public void stop() {
  }

}
