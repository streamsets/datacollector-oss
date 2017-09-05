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

import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.gpudb.BulkInserter;
import com.gpudb.BulkInserter.InsertException;
import com.gpudb.Type;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.kinetica.util.KineticaRecordUtils;

public class KineticaTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(KineticaTarget.class);
  private final KineticaConfigBean conf;
  private ErrorRecordHandler errorRecordHandler;

  // The Kinetica Type for the table
  private Type type = null;

  // Kinetica Bulk Inserter
  private BulkInserter<IndexedRecord> bulkInserter = null;

  KineticaTarget(KineticaConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    Target.Context context = getContext();
    errorRecordHandler = new DefaultErrorRecordHandler(context);

    if (issues.size() == 0) {

      // Init our Kinetica helper
      KineticaHelper kineticaHelper = new KineticaHelper(conf, context, issues);

      // Get the Kinetica Type for our table
      type = kineticaHelper.getType();

      // Get the Kinetica Bulk Inserter
      bulkInserter = kineticaHelper.getBulkInserter();

    }
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> batchIterator = batch.getRecords();
    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      try {
        write(record);
      } catch (Exception e) {
        switch (getContext().getOnErrorRecord()) {
        case DISCARD:
          break;
        case TO_ERROR:
           errorRecordHandler.onError(new OnRecordErrorException(record, Errors.KINETICA_03, e));
          break;
        case STOP_PIPELINE:
          errorRecordHandler.onError(Lists.newArrayList(batch.getRecords()), new StageException(Errors.KINETICA_03, e));
          break;
        default:
          throw new IllegalStateException(
            Utils.format("Unknown OnError value '{}'", getContext().getOnErrorRecord(), e));
        }
      }
    }

    // Make sure to flush the Kinetica BulkInserter after each batch
    try {
      bulkInserter.flush();
    } catch (InsertException e) {
      LOG.error(e.getMessage(), e);
      errorRecordHandler.onError(Lists.newArrayList(batch.getRecords()), new StageException(Errors.KINETICA_04, e));
    }
  }

  private void write(Record record) throws StageException {

    // Create a Kinetica record from the StreamSets record
    com.gpudb.Record kineticaRecord = KineticaRecordUtils.createKineticaRecordFromStreamsetsRecord(record, type);

    if (kineticaRecord != null) {
      // Add the record to the Kinetica Bulk Inserter
      try {
        bulkInserter.insert(kineticaRecord);
      } catch (InsertException e) {
        errorRecordHandler.onError(new OnRecordErrorException(record, Errors.KINETICA_03, e));
      }
    }
  }
}
