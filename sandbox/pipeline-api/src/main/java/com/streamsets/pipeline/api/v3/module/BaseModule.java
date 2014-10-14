/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
