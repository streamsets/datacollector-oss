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
import com.streamsets.pipeline.hbase.api.common.processor.HBaseLookupConfig;
import com.streamsets.pipeline.hbase.api.common.producer.ColumnInfo;
import com.streamsets.pipeline.hbase.api.common.producer.HBaseConnectionConfig;
import com.streamsets.pipeline.hbase.api.common.producer.StorageType;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class THBaseProvider implements HBaseProvider {

  @Override
  public HBaseProducer createProducer(
      Stage.Context context, HBaseConnectionConfig hBaseConnectionConfig, ErrorRecordHandler errorRecordHandler
  ) {
    return new HBaseProducer() {
      @Override
      public Date setBatchTime() {
        return null;
      }

      @Override
      public Date getRecordTime(Record record, String timeDriver) throws OnRecordErrorException {
        return null;
      }

      @Override
      public void checkHBaseAvailable(List<Stage.ConfigIssue> issues) {

      }

      @Override
      public void validateQuorumConfigs(List<Stage.ConfigIssue> issues) {

      }

      @Override
      public HBaseConnectionHelper getHBaseConnectionHelper() {
        return null;
      }

      @Override
      public void createTable(String tableName) throws InterruptedException, IOException {

      }

      @Override
      public void destroyTable() {

      }

      @Override
      public void writeRecordsInHBase(
          Batch batch,
          StorageType rowKeyStorageType,
          String hbaseRowKey,
          String timeDriver,
          Map<String, ColumnInfo> columnMappings,
          boolean ignoreMissingField,
          boolean implicitFieldMapping,
          boolean ignoreInvalidColumn
      ) throws StageException {

      }
    };
  }

  @Override
  public HBaseProcessor createProcessor(
      Stage.Context context, HBaseLookupConfig hBaseLookupConfig, ErrorRecordHandler errorRecordHandler
  ) {
    return new HBaseProcessor() {
      @Override
      public HBaseConnectionHelper getHBaseConnectionHelper() {
        return null;
      }

      @Override
      public void createTable() throws InterruptedException, IOException {

      }

      @Override
      public void destroyTable() {

      }

      @Override
      public Result get(Get get) throws IOException {
        return null;
      }

      @Override
      public Result[] get(List<Get> gets) throws IOException {
        return new Result[0];
      }
    };
  }
}
