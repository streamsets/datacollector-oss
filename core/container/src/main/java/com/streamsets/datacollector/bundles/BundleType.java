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
  SUPPORT("sdc.supportBundle", false, "", "bundle_"),

  // Anonymously submitted statistic bundle
  STATS("sdc.usageStats", true, "stats/", "stats_"),
  ;

  private final String tag;
  private final boolean anonymizeMetadata;
  private final String pathPrefix;
  private final String namePrefix;
  BundleType(
    String tag,
    boolean anonymizeMetadata,
    String pathPrefix,
    String namePrefix
  ) {
    this.tag = tag;
    this.anonymizeMetadata = anonymizeMetadata;
    this.pathPrefix = pathPrefix;
    this.namePrefix = namePrefix;
  }

  public String getTag() {
    return tag;
  }

  public boolean isAnonymizeMetadata() {
    return anonymizeMetadata;
  }

  public String getPathPrefix() {
    return pathPrefix;
  }

  public String getNamePrefix() {
    return namePrefix;
  }
}
