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

import java.io.InputStream;

public class SupportBundle {

  /**
   * Full key for StreamSets backend services (e.g. $date/$bundleName).
   */
  private final String bundleKey;

  /**
   * Bundle file name including suffix.
   */
  private final String bundleName;

  /**
   * Stream with bundle data.
   */
  private final InputStream inputStream;

  public SupportBundle(
    String bundleKey,
    String bundleName,
    InputStream inputStream
  ) {
    this.bundleKey = bundleKey;
    this.bundleName = bundleName;
    this.inputStream = inputStream;
  }

  public String getBundleKey() {
    return bundleKey;
  }

  public String getBundleName() {
    return bundleName;
  }

  public InputStream getInputStream() {
    return inputStream;
  }
}
