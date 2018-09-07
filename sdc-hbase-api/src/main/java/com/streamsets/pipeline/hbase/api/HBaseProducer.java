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

package com.streamsets.pipeline.hbase.api;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.hbase.api.common.producer.ColumnInfo;
import com.streamsets.pipeline.hbase.api.common.producer.StorageType;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public interface HBaseProducer {

  /**
   * Set the new batch time to the current date
   */
  Date setBatchTime();

  /**
   * Returns the record time
   *
   * @param record record from time will be retrieved
   * @param timeDriver Time basis to use for cell timestamps
   * @return record time
   *
   * @throws OnRecordErrorException
   */
  Date getRecordTime(Record record, String timeDriver) throws OnRecordErrorException;

  /**
   * Call HBase method to check if it is available
   *
   * @param issues List of initialization issues
   */
  void checkHBaseAvailable(List<Stage.ConfigIssue> issues);

  /**
   * Validate all the configs related to Quorum
   *
   * @param issues list of initialization issues
   */
  void validateQuorumConfigs(List<Stage.ConfigIssue> issues);

  /**
   * Fetches the HBaseConnectionHelper
   *
   * @return HBaseConnectionHelper
   */
  HBaseConnectionHelper getHBaseConnectionHelper();

  /**
   * Creates a HBase table
   *
   * @param tableName Name of the table
   * @throws InterruptedException
   * @throws IOException
   */
  void createTable(String tableName) throws InterruptedException, IOException;

  /**
   * Destroy the existing HBaseTable
   */
  void destroyTable();

  /**
   * Writes all the records in the batch into HBase
   *
   * @param batch batch cotaining all records
   * @param rowKeyStorageType Row key sotrage type
   * @param hbaseRowKey Row key
   * @param timeDriver Time expression
   * @param columnMappings Map of columns
   * @param ignoreMissingField boolean to ignore missing field
   * @param implicitFieldMapping boolean to implicitly map fields
   * @param ignoreInvalidColumn boolean to ignore invalid columns
   * @throws StageException
   */
  void writeRecordsInHBase(
      Batch batch,
      StorageType rowKeyStorageType,
      String hbaseRowKey,
      String timeDriver,
      Map<String, ColumnInfo> columnMappings,
      boolean ignoreMissingField,
      boolean implicitFieldMapping,
      boolean ignoreInvalidColumn
  ) throws StageException;
}
