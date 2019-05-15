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
package com.streamsets.pipeline.stage.kinetica.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
  private List<String> customWorkerUrlList;

  public KineticaBulkInserterUtils(GPUdb gpudb, Type type, KineticaConfigBean conf) {
    this.gpudb = gpudb;
    this.type = type;
    this.tableName = conf.tableName;
    this.batchSize = conf.batchSize;
    this.ipRegex = conf.ipRegex;
    this.isMultiheadIngestDisabled = conf.disableMultihead;
    this.isUpdateOnExistingPrimaryKey = conf.updateOnExistingPk;
    this.customWorkerUrlList = conf.customWorkerUrlList;
  }

  public BulkInserter<IndexedRecord> createBulkInserter() throws GPUdbException {

    BulkInserter.WorkerList workers = null;

    // if multi-head-ingest is disabled, leave workers as null
    if (isMultiheadIngestDisabled){
      LOG.info("Kinetica multi-head ingest is disabled");
    } else {
      LOG.info("Kinetica multi-head ingest is enabled");

      // Use a Custom Worker URL List if set
      if(customWorkerUrlList != null && !customWorkerUrlList.isEmpty()){
        LOG.info("Using customWorkerUrlList: {}", String.join(",", customWorkerUrlList));
        workers = new BulkInserter.WorkerList();
        for (String workerUrl : customWorkerUrlList){
          try{
            workerUrl = workerUrl.trim();
            if (workerUrl.length() == 0){
              throw new GPUdbException("Error: Custom Worker URL entry cannot be an empty string");
            }
            URL url = new URL(workerUrl);
            workers.add(url);
          } catch(MalformedURLException e){
            LOG.error("Malformed URL in customWorkerUrlList");
            throw new GPUdbException(e.getMessage());
          }
        }
      } else {
        // see if we need to filter worker host names
        if (ipRegex != null && ipRegex.trim().length() > 0) {
          LOG.info("Using ipRegex: '" + ipRegex + "'");
          Pattern pattern = Pattern.compile(ipRegex);
          workers = new BulkInserter.WorkerList(gpudb, pattern);
        } else {
          workers = new BulkInserter.WorkerList(gpudb);
        }
      }
    }

    if (workers != null){
      for (Iterator<URL> iter = workers.iterator(); iter.hasNext();) {
        LOG.info("GPUdb BulkInserter worker: " + iter.next());
      }
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
