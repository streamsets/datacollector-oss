/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v1;

public abstract class AbstractModule implements Module {
  private Info info;
  private Context context;

  @Override
  public final void init(Info info, Context context) {
    this.info = info;
    this.context = context;
    init();
  }

  protected Info getInfo() {
    return info;
  }

  protected Context getContext() {
    return context;
  }

  protected void init() {
  }

  @Override
  public void destroy() {
  }

}
