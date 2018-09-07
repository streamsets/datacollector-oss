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

package com.streamsets.pipeline.hbase.impl;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.hbase.api.common.processor.HBaseLookupConfig;
import com.streamsets.pipeline.hbase.api.impl.AbstractHBaseConnectionHelper;
import com.streamsets.pipeline.hbase.api.impl.AbstractHBaseProcessor;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

public class HBaseProcessor0_98 extends AbstractHBaseProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseProcessor0_98.class);

  private HTable table = null;

  public HBaseProcessor0_98(
      Stage.Context context,
      AbstractHBaseConnectionHelper hbaseConnectionHelper,
      HBaseLookupConfig hBaseLookupConfig,
      ErrorRecordHandler errorRecordHandler
  ) {
    super(context, hbaseConnectionHelper, hBaseLookupConfig, errorRecordHandler);
  }

  @Override
  public void destroyTable() {
    if (table != null) {
      try {
        hbaseConnectionHelper.getUGI().doAs((PrivilegedExceptionAction<Void>) () -> {
          table.close();
          return null;
        });

      } catch (InterruptedException | IOException ex) {
        LOG.debug("error closing HBase table {}", ex.getMessage(), ex);
      }
    }
  }

  @Override
  public Result get(Get get) throws IOException {
    return table.get(get);
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    return table.get(gets);
  }

  @Override
  public void createTable() throws InterruptedException, IOException {
    hbaseConnectionHelper.getUGI().doAs((PrivilegedExceptionAction<Void>) () -> {
      table = new HTable(hbaseConnectionHelper.getHBaseConfiguration(),
          hBaseLookupConfig.hBaseConnectionConfig.tableName
      );
      return null;
    });
  }
}
