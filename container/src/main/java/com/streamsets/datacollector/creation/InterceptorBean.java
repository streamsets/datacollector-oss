/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.creation;

import com.streamsets.datacollector.config.InterceptorDefinition;
import com.streamsets.datacollector.stagelibrary.ClassLoaderReleaser;
import com.streamsets.pipeline.api.interceptor.Interceptor;

/**
 * Runtime bean for interceptor that contains instance of the interceptor itself as well as it's configuration.
 */
public class InterceptorBean {
  private final InterceptorDefinition definition;
  private final Interceptor interceptor;
  private final ClassLoaderReleaser classLoaderReleaser;

  public InterceptorBean(
    InterceptorDefinition definition,
    Interceptor interceptor,
    ClassLoaderReleaser classLoaderReleaser
  ) {
    this.definition = definition;
    this.interceptor = interceptor;
    this.classLoaderReleaser = classLoaderReleaser;
  }

  public InterceptorDefinition getDefinition() {
    return definition;
  }

  public Interceptor getInterceptor() {
    return interceptor;
  }

  public void releaseClassLoader() {
    classLoaderReleaser.releaseStageClassLoader(interceptor.getClass().getClassLoader());
  }

  public String getMetricName() {
    return getDefinition().getKlass().getName().replace(".", "_");
  }
}
