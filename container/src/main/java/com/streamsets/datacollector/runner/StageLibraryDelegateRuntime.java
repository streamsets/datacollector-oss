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
package com.streamsets.datacollector.runner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.util.LambdaUtil;
import com.streamsets.pipeline.api.delegate.StageLibraryDelegate;

import java.util.Set;

public class StageLibraryDelegateRuntime implements Runnable {

  // Static list with all supported delegates
  private static Set<Class> SUPPORTED_DELEGATES = ImmutableSet.of(
    Runnable.class
  );

  /**
   * Return true if and only given delegate interface is supported by this instance.
   *
   * @param exportedInterface Service interface
   * @return True if given interface is supported by this runtime
   */
  public static boolean supports(Class exportedInterface) {
    return SUPPORTED_DELEGATES.contains(exportedInterface);
  }

  private ClassLoader classLoader;
  private StageLibraryDelegate wrapped;

  public StageLibraryDelegateRuntime(
    ClassLoader classLoader,
    StageLibraryDelegate wrapped
  ) {
    this.classLoader = classLoader;
    this.wrapped = wrapped;
  }

  @VisibleForTesting
  public StageLibraryDelegate getWrapped() {
    return wrapped;
  }

  @Override // Runnable
  public void run() {
    LambdaUtil.privilegedWithClassLoader(
      classLoader,
      () -> { ((Runnable)wrapped).run(); return null; }
    );
  }
}
