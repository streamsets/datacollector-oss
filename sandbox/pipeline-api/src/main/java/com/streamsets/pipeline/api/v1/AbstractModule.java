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
