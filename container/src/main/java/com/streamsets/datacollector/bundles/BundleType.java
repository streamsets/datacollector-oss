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
package com.streamsets.datacollector.bundles;

public enum BundleType {
  // Bundle primarily meant for support with relevant information to troubleshoot user issue
  SUPPORT("sdc.supportBundle", true),

  // Anonymously submitted statistic bundle
  STATS("sdc.usageStats", false),
  ;

  private final String tag;
  private final boolean createMetadataProperties;
  BundleType(String tag, boolean createMetadataProperties) {
    this.tag = tag;
    this.createMetadataProperties = createMetadataProperties;
  }

  public String getTag() {
    return tag;
  }

  public boolean isCreateMetadataProperties() {
    return createMetadataProperties;
  }
}
