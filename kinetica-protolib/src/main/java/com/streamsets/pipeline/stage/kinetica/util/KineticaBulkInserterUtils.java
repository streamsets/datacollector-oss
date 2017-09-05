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
package com.streamsets.pipeline.stage.kinetica.util;

import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.Type;
import com.gpudb.protocol.InsertRecordsRequest;
import com.streamsets.pipeline.stage.kinetica.KineticaConfigBean;

public class KineticaBulkInserterUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KineticaBulkInserterUtils.class);

  private GPUdb gpudb;
  private Type type;
  private int batchSize;
  private String tableName;
  private String ipRegex;
  boolean isMultiheadIngestDisabled;
  boolean isUpdateOnExistingPrimaryKey;

  public KineticaBulkInserterUtils(GPUdb gpudb, Type type, KineticaConfigBean conf) {
    this.gpudb = gpudb;
    this.type = type;
    this.tableName = conf.tableName;
    this.batchSize = conf.batchSize;
    this.ipRegex = conf.ipRegex;
    this.isMultiheadIngestDisabled = conf.disableMultihead;
    this.isUpdateOnExistingPrimaryKey = conf.updateOnExistingPk;
  }

  public BulkInserter<IndexedRecord> createBulkInserter() throws GPUdbException {

    BulkInserter.WorkerList workers = null;

    // see if we need to filter worker host names
    if (ipRegex != null) {
      Pattern pattern = Pattern.compile(ipRegex);
      workers = new BulkInserter.WorkerList(gpudb, pattern);
    } else {
      workers = new BulkInserter.WorkerList(gpudb);
    }

    for (Iterator<URL> iter = workers.iterator(); iter.hasNext();) {
      LOG.info("GPUdb BulkInserter worker: " + iter.next());
    }

    HashMap<String, String> options = new HashMap<String, String>();

    if (isUpdateOnExistingPrimaryKey) {
      LOG.info("Setting update_on_existing_pk = true");
      options.put("update_on_existing_pk", InsertRecordsRequest.Options.TRUE);
    } else {
      LOG.info("Setting update_on_existing_pk = false");
      options.put("update_on_existing_pk", InsertRecordsRequest.Options.FALSE);
    }

    LOG.info("Creating BulkInserter with batch size: " + batchSize);

    return new BulkInserter<IndexedRecord>(gpudb, tableName, type, batchSize, options, workers);
  }

}
