/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.bundles;

import java.io.IOException;

/**
 * General interface for classes that generates content into the bundle.
 */
public interface BundleContentGenerator {

  /**
   * Initialize the generator (anything that is required)
   */
  public default void init(BundleContext context) {}

  /**
   * Generate the actual content.
   *
   * @param context Context object for this particular bundle.
   * @param writer Output writer where the results should written to
   */
  public void generateContent(BundleContext context, BundleWriter writer) throws IOException;


  /**
   * Destroy the generator (clean any lingering stuff).
   */
  public default void destroy(BundleContext context) {}

}
