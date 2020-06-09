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

package com.streamsets.pipeline.stage.origin.jdbc.table;

import java.util.List;
import java.util.Map;

public interface TableConfigBean {
  String DEFAULT_PARTITION_SIZE = "1000000";
  int DEFAULT_MAX_NUM_ACTIVE_PARTITIONS = -1;
  String PARTITIONING_MODE_FIELD = "partitioningMode";
  String MAX_NUM_ACTIVE_PARTITIONS_FIELD = "maxNumActivePartitions";
  String PARTITION_SIZE_FIELD = "partitionSize";
  String PARTITIONING_MODE_DEFAULT_VALUE_STR = "DISABLED";
  PartitioningMode PARTITIONING_MODE_DEFAULT_VALUE = PartitioningMode.valueOf(PARTITIONING_MODE_DEFAULT_VALUE_STR);
  String ENABLE_NON_INCREMENTAL_FIELD = "enableNonIncremental";
  boolean IS_TABLE_PATTERN_LIST_PROVIDED_DEFAULT_VALUE = false;
  boolean ENABLE_NON_INCREMENTAL_DEFAULT_VALUE = true;
  String ALLOW_LATE_TABLE = "commonSourceConfigBean.allowLateTable";
  String QUERY_INTERVAL_FIELD = "commonSourceConfigBean.queryInterval";
  String QUERIES_PER_SECOND_FIELD = "commonSourceConfigBean.queriesPerSecond";
  String DBO = "dbo";

  String getSchema();

  boolean isTablePatternListProvided();

  String getTablePattern();

  List<String> getTablePatternList();

  String getTableExclusionPattern();

  String getSchemaExclusionPattern();

  boolean isOverrideDefaultOffsetColumns();

  List<String> getOffsetColumns();

  Map<String, String> getOffsetColumnToInitialOffsetValue();

  boolean isEnableNonIncremental();

  PartitioningMode getPartitioningMode();

  String getPartitionSize();

  int getMaxNumActivePartitions();

  String getExtraOffsetColumnConditions();
}
