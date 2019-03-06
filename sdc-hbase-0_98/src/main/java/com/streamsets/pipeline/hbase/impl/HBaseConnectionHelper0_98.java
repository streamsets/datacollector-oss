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
import com.streamsets.pipeline.hbase.api.common.Errors;
import com.streamsets.pipeline.hbase.api.common.producer.HBaseColumn;
import com.streamsets.pipeline.hbase.api.impl.AbstractHBaseConnectionHelper;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HBaseConnectionHelper0_98 extends AbstractHBaseConnectionHelper {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseConnectionHelper0_98.class);

  @Override
  public HTableDescriptor checkConnectionAndTableExistence(
      List<Stage.ConfigIssue> issues, Stage.Context context, String hbaseName, String tableName
  ) {
    LOG.debug("Validating connection to hbase cluster and whether table " + tableName + " exists and is enabled");
    HTableDescriptor hTableDescriptor = null;
    try (HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConfiguration)) {
      if (!hbaseAdmin.tableExists(tableName)) {
        issues.add(context.createConfigIssue(hbaseName, TABLE_NAME, Errors.HBASE_07, tableName));
      } else if (!hbaseAdmin.isTableEnabled(tableName)) {
        issues.add(context.createConfigIssue(hbaseName, TABLE_NAME, Errors.HBASE_08, tableName));
      } else {
        hTableDescriptor = hbaseAdmin.getTableDescriptor(TableName.valueOf(tableName));
      }
    } catch (Exception ex) {
      LOG.warn("Received exception while connecting to cluster: ", ex);
      issues.add(context.createConfigIssue(hbaseName, null, Errors.HBASE_06, ex.toString(), ex));
    }
    return hTableDescriptor;
  }

  @Override
  public HBaseColumn getColumn(String column) {
    HBaseColumn result = new HBaseColumn();
    byte[][] parts = KeyValue.parseColumn(Bytes.toBytes(column));

    if (parts.length == 2) {
      result.setCf(parts[0]);
      result.setQualifier(parts[1]);
    }
    return result;
  }
}
