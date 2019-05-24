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
package com.streamsets.pipeline.stage.destination.maprdb;

import com.streamsets.pipeline.hbase.api.common.producer.HBaseConnectionConfig;
import com.streamsets.pipeline.hbase.api.common.producer.StorageType;
import com.streamsets.pipeline.stage.destination.hbase.HBaseFieldMappingConfig;
import com.streamsets.pipeline.stage.destination.hbase.HBaseTarget;

import java.util.List;

/**
 * Overridden HBaseTarget that disables checks that are not relevant to Mapr DB (zookeeper and such).
 */
public class MapRDBTarget extends HBaseTarget {

  public MapRDBTarget(
        HBaseConnectionConfig conf,
        String hbaseRowKey,
        StorageType rowKeyStorageType,
        List<HBaseFieldMappingConfig> hbaseFieldColumnMapping,
        boolean implicitFieldMapping,
        boolean ignoreMissingFieldPath,
        boolean ignoreInvalidColumn,
        boolean validateTableExistence,
        String timeDriver
  ) {
    super(
      conf,
      hbaseRowKey,
      rowKeyStorageType,
      hbaseFieldColumnMapping,
      implicitFieldMapping,
      ignoreMissingFieldPath,
      ignoreInvalidColumn,
      validateTableExistence,
      timeDriver
    );
  }

  @Override
  protected boolean isReallyHBase() {
    return false;
    /* In case of a MapRDB, we don't want to do some operations and validations.
    - Disable zooekeepr checks because zookeeper is not needed and we're hiding it's configuration completely
    - Call to HBaseAdmin.checkHBaseAvailable is not supported on MapR DB
     http://doc.mapr.com/display/MapR/Support+for+the+HBaseAdmin+class */
  }
}
