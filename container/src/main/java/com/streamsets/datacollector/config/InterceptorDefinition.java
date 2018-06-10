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
package com.streamsets.datacollector.config;

import com.streamsets.pipeline.api.interceptor.Interceptor;
import com.streamsets.pipeline.api.interceptor.InterceptorCreator;

public class InterceptorDefinition implements PrivateClassLoaderDefinition {
  private final StageLibraryDefinition libraryDefinition;
  private final Class<? extends Interceptor> klass;
  private final ClassLoader classLoader;

  private final int version;
  private final Class<? extends InterceptorCreator> defaultCreator;

  public InterceptorDefinition(
    StageLibraryDefinition libraryDefinition,
    Class<? extends Interceptor> klass,
    ClassLoader classLoader,
    int version,
    Class<? extends InterceptorCreator> defaultCreator
  ) {
    this.libraryDefinition = libraryDefinition;
    this.klass = klass;
    this.classLoader = classLoader;
    this.version = version;
    this.defaultCreator = defaultCreator;
  }

  public StageLibraryDefinition getLibraryDefinition() {
    return libraryDefinition;
  }

  public Class<? extends Interceptor> getKlass() {
    return klass;
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }

  public int getVersion() {
    return version;
  }

  public Class<? extends InterceptorCreator> getDefaultCreator() {
    return defaultCreator;
  }

  @Override
  public String getName() {
    return klass.getName();
  }

  @Override
  public ClassLoader getStageClassLoader() {
    return classLoader;
  }

  @Override
  public boolean isPrivateClassLoader() {
    // We don't currently support private class loaders on interceptor
    return false;
  }
}
