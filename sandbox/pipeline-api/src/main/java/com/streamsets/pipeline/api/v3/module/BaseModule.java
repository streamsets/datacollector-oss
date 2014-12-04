/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.module;

import com.streamsets.pipeline.api.v3.Module;
import com.streamsets.pipeline.api.v3.Module.Context;

public abstract class BaseModule<C extends Context> implements Module<C> {
  private Info info;
  private C context;
  private boolean previewMode;

  @Override
  public final void init(Info info, C context, boolean previewMode) {
    this.info = info;
    this.context = context;
    this.previewMode = previewMode;
    init();
  }

  @Override
  public Info getInfo() {
    return info;
  }

  protected C getContext() {
    return context;
  }

  public boolean isPreviewMode() {
    return previewMode;
  }

  protected void init() {
  }

  @Override
  public void destroy() {
  }

}
