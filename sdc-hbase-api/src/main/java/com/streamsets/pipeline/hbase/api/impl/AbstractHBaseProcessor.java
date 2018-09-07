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

package com.streamsets.pipeline.hbase.api.impl;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.hbase.api.HBaseConnectionHelper;
import com.streamsets.pipeline.hbase.api.HBaseProcessor;
import com.streamsets.pipeline.hbase.api.common.processor.HBaseLookupConfig;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public abstract class AbstractHBaseProcessor implements HBaseProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseProcessor.class);

  protected Stage.Context context;
  protected HBaseConnectionHelper hbaseConnectionHelper;
  protected final HBaseLookupConfig hBaseLookupConfig;
  protected ErrorRecordHandler errorRecordHandler;

  public AbstractHBaseProcessor(
      Stage.Context context,
      AbstractHBaseConnectionHelper hbaseConnectionHelper,
      HBaseLookupConfig hBaseLookupConfig,
      ErrorRecordHandler errorRecordHandler
  ) {
    this.context = context;
    this.hbaseConnectionHelper = hbaseConnectionHelper;
    this.hBaseLookupConfig = hBaseLookupConfig;
    this.errorRecordHandler = errorRecordHandler;
  }

  public abstract void createTable() throws IOException, InterruptedException;

  public abstract void destroyTable();

  public abstract Result get(Get get) throws IOException;

  public abstract Result[] get(List<Get> gets) throws IOException;

  public HBaseConnectionHelper getHBaseConnectionHelper() {
    return hbaseConnectionHelper;
  }
}
