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
package com.streamsets.pipeline.stage.lib.hive.cache;

/**
 * Represents different type of cache types supported by the instance of {@link HMSCache}
 */
public enum HMSCacheType {
  TBLPROPERTIES_INFO(new TBLPropertiesInfoCacheSupport()),
  TYPE_INFO(new TypeInfoCacheSupport()),
  PARTITION_VALUE_INFO(new PartitionInfoCacheSupport()),
  AVRO_SCHEMA_INFO(new AvroSchemaInfoCacheSupport());

  HMSCacheSupport support;

  HMSCacheType(HMSCacheSupport support) {
    this.support = support;
  }

  /**
   * Get the supporting implementation {@link HMSCacheSupport} for {@link HMSCacheType}
   * @param <T> Returns {@link HMSCacheSupport}
   * @return {@link HMSCacheSupport}
   */
  @SuppressWarnings("unchecked")
  public <T extends HMSCacheSupport> T getSupport() {
    return (T) support;
  }
}
