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
package com.streamsets.pipeline.stage.kinetica;

import java.util.List;

import org.apache.avro.generic.IndexedRecord;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.Type;
import com.streamsets.pipeline.api.Stage.ConfigIssue;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.stage.kinetica.util.KineticaBulkInserterUtils;
import com.streamsets.pipeline.stage.kinetica.util.KineticaConnectionUtils;
import com.streamsets.pipeline.stage.kinetica.util.KineticaTableUtils;

/*
 * Helper class that when created will validate the database URL, retrieve
 * metadata for the table named in the KineticaConfigBean and create a
 * BulkInserter
 */

public class KineticaHelper {

  // The Type we will create for the table
  Type type = null;

  // The BulkInserter we will create for the table
  BulkInserter<IndexedRecord> bulkInserter = null;

  public Type getType() {
    return type;
  }

  public BulkInserter<IndexedRecord> getBulkInserter() {
    return bulkInserter;
  }

  /*
   * The KineticaHelper Constructor will attempt to connect to the Kinetica
   * database, retrieve metadata for the specified table and create a
   * BulkInserter.
   */
  public KineticaHelper(KineticaConfigBean conf, Target.Context context, List<ConfigIssue> issues) {

    // Connect to the database
    try {
      GPUdb gpudb = initConnection(conf);

      // Get metadata for the table
      try {
        getTableMetadata(gpudb, conf.tableName);

        // Create a BulkInserter
        try {
          bulkInserter = createBulkInserter(gpudb, type, conf);

        } catch (GPUdbException e) {
          // Exception creating BulkInserter
          issues.add(context.createConfigIssue(null, null, Errors.KINETICA_02, e.toString()));
        }

      } catch (GPUdbException e) {
        // Exception creating the type for the table
        issues.add(context.createConfigIssue(Groups.TABLE.name(), conf.url, Errors.KINERICA_01, e.toString()));
      }

    } catch (GPUdbException e) {
      // Exception connecting to GPUdb
      issues.add(context.createConfigIssue(Groups.CONNECTION.name(), conf.url, Errors.KINETICA_00, e.toString()));
    }

  }

  // Connect to the database
  private GPUdb initConnection(KineticaConfigBean conf) throws GPUdbException {
    KineticaConnectionUtils kineticaConnectionUtils = new KineticaConnectionUtils();
    return kineticaConnectionUtils.getGPUdb(conf);
  }

  // Get metadata for the table
  private void getTableMetadata(GPUdb gpudb, String tableName) throws GPUdbException {
    KineticaTableUtils kineticaTableUtils = new KineticaTableUtils(gpudb, tableName);
    type = kineticaTableUtils.getType();
  }

  // Create a BulkInserter
  private BulkInserter<IndexedRecord> createBulkInserter(GPUdb gpudb, Type type, KineticaConfigBean conf)
      throws GPUdbException {
    KineticaBulkInserterUtils kineticaBulkInserterUtils = new KineticaBulkInserterUtils(gpudb, type, conf);
    return kineticaBulkInserterUtils.createBulkInserter();
  }

}
