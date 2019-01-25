/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline;

/**
 * A class that performs some kind of initialization, before the {@link BootstrapMain#main(String[])} method is called.
 * Any implementation of this interface must have a no-arg constructor defined since it will be created by reflection.
 */
public interface BootstrapInitializer {

  /**
   *
   * Perform any initialization steps required before the bootstrap runs.  This will be invoked in an isolated classloader.
   *
   * @param originalArgs the original argument passed to {@link BootstrapMain#main(String[])}
   * @return the args that will be passed to {@link BootstrapMain#bootstrap(String[])}
   */
  String[] initialize(String[] originalArgs);
}
