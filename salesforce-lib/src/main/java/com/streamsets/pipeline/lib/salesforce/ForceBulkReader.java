/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.lib.salesforce;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchInfoList;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.streamsets.pipeline.api.StageException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 ForceBulkReader has all of the logic required to pull records from the
 Salesforce Bulk API, so it can be used from both the origin and lookup
 processor.
 */
public class ForceBulkReader {
  private static final Logger LOG = LoggerFactory.getLogger(ForceBulkReader.class);
  private static final String SFORCE_ENABLE_PKCHUNKING = "Sforce-Enable-PKChunking";
  private static final String CHUNK_SIZE = "chunkSize";
  private static final String START_ROW = "startRow";
  private static final String RECORDS = "records";

  // Bulk API state
  private BatchInfo batch;
  private JobInfo job;
  private QueryResultList queryResultList;
  private int resultIndex;
  private XMLEventReader xmlEventReader;
  private Set<String> processedBatches;
  private BatchInfoList batchList;
  private XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();

  private final ForceStage stage;

  public ForceBulkReader(
      ForceStage stage
  ) {
    this.stage = stage;

    xmlInputFactory.setProperty(XMLInputFactory.IS_COALESCING, true);
    xmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
  }

  public String produce(String nextSourceOffset, int maxBatchSize, ForceCollector collector) throws StageException {
    BulkConnection bulkConnection = stage.getBulkConnection();

    if (job == null) {
      // No job in progress - start from scratch
      try {
        collector.init();

        final String preparedQuery = collector.prepareQuery(nextSourceOffset);
        LOG.info("SOQL Query is: {}", preparedQuery);

        if (collector.isDestroyed()) {
          throw new StageException(collector.isPreview() ? Errors.FORCE_25 : Errors.FORCE_26);
        }
        createJob(((SobjectRecordCreator)stage.getRecordCreator()).sobjectType, bulkConnection);
        LOG.info("Created Bulk API job {}", job.getId());

        if (collector.isDestroyed()) {
          throw new StageException(collector.isPreview() ? Errors.FORCE_25 : Errors.FORCE_26);
        }
        BatchInfo batch;
        try {
          batch = bulkConnection.createBatchFromStream(job,
              new ByteArrayInputStream(preparedQuery.getBytes(StandardCharsets.UTF_8)));
        } catch (AsyncApiException e) {
          ForceUtils.renewSession(bulkConnection, e);
          batch = bulkConnection.createBatchFromStream(job,
              new ByteArrayInputStream(preparedQuery.getBytes(StandardCharsets.UTF_8)));
        }
        LOG.info("Created Bulk API batch {}", batch.getId());
        processedBatches = new HashSet<>();
      } catch (AsyncApiException e) {
        throw new StageException(Errors.FORCE_01, e);
      }
    }

    // We started the job already, see if the results are ready
    // Loop here so that we can wait for results in preview mode and not return an empty batch
    // Preview will cut us off anyway if we wait too long
    while (queryResultList == null) {
      if (collector.isDestroyed()) {
        throw new StageException(collector.isPreview() ? Errors.FORCE_25 : Errors.FORCE_26);
      }

      try {
        // PK Chunking gives us multiple batches - process them in turn
        try {
          batchList = bulkConnection.getBatchInfoList(job.getId());
        } catch (AsyncApiException e) {
          ForceUtils.renewSession(bulkConnection, e);
          batchList = bulkConnection.getBatchInfoList(job.getId());
        }
        for (BatchInfo batchInfo : batchList.getBatchInfo()) {
          if (batchInfo.getState() == BatchStateEnum.Failed) {
            LOG.error("Batch {} failed: {}", batchInfo.getId(), batchInfo.getStateMessage());
            throw new StageException(Errors.FORCE_03, batchInfo.getStateMessage());
          } else if (!processedBatches.contains(batchInfo.getId())) {
            if (batchInfo.getState() == BatchStateEnum.NotProcessed) {
              // Skip this batch - it's the 'original batch' in PK chunking
              // See https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_code_curl_walkthrough_pk_chunking.htm
              LOG.info("Batch {} not processed", batchInfo.getId());
              processedBatches.add(batchInfo.getId());
            } else if (batchInfo.getState() == BatchStateEnum.Completed) {
              LOG.info("Batch {} completed", batchInfo.getId());
              batch = batchInfo;
              try {
                queryResultList = bulkConnection.getQueryResultList(job.getId(), batch.getId());
              } catch (AsyncApiException e) {
                ForceUtils.renewSession(bulkConnection, e);
                queryResultList = bulkConnection.getQueryResultList(job.getId(), batch.getId());
              }
              LOG.info("Query results: {}", queryResultList.getResult());
              resultIndex = 0;
              break;
            }
          }
        }
        if (queryResultList == null) {
          // Bulk API is asynchronous, so wait a little while...
          try {
            // TBD config
            LOG.info("Waiting {} milliseconds for job {}", 2000, job.getId());
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            LOG.debug("Interrupted while sleeping");
            Thread.currentThread().interrupt();
          }
          if (!collector.isPreview()) { // If we're in preview, then don't return an empty batch!
            LOG.info("Job {} in progress", job.getId());
            return nextSourceOffset;
          }
        }
      } catch (AsyncApiException e) {
        throw new StageException(Errors.FORCE_02, e);
      }
    }

    if (xmlEventReader == null && queryResultList != null) {
      // We have results - retrieve the next one!
      String resultId = queryResultList.getResult()[resultIndex];
      resultIndex++;

      try {
        try {
          xmlEventReader = xmlInputFactory.createXMLEventReader(bulkConnection.getQueryResultStream(job.getId(), batch.getId(), resultId));
        } catch (AsyncApiException e) {
          ForceUtils.renewSession(bulkConnection, e);
          xmlEventReader = xmlInputFactory.createXMLEventReader(bulkConnection.getQueryResultStream(job.getId(), batch.getId(), resultId));
        }
      } catch (AsyncApiException e) {
        throw new StageException(Errors.FORCE_05, e);
      } catch (XMLStreamException e) {
        throw new StageException(Errors.FORCE_36, e);
      }
    }

    if (xmlEventReader != null){
      int numRecords = 0;
      while (xmlEventReader.hasNext() && numRecords < maxBatchSize) {
        try {
          XMLEvent event = xmlEventReader.nextEvent();
          if (event.isStartElement() && event.asStartElement().getName().getLocalPart().equals(RECORDS)) {
            // Pull the root map from the reader
            String offset = collector.addRecord(((BulkRecordCreator)stage.getRecordCreator()).getFields(xmlEventReader), numRecords);
            nextSourceOffset = ForceUtils.RECORD_ID_OFFSET_PREFIX + offset;
            ++numRecords;
          }
        } catch (XMLStreamException e) {
          throw new StageException(Errors.FORCE_37, e);
        }
      }

      if (!xmlEventReader.hasNext()) {
        // Exhausted this result - come back in on the next batch
        xmlEventReader = null;
        if (resultIndex == queryResultList.getResult().length) {
          // We're out of results, too!
          processedBatches.add(batch.getId());
          queryResultList = null;
          batch = null;
          if (processedBatches.size() == batchList.getBatchInfo().length) {
            // And we're done with the job
            try {
              try {
                bulkConnection.closeJob(job.getId());
              } catch (AsyncApiException e) {
                ForceUtils.renewSession(bulkConnection, e);
                bulkConnection.closeJob(job.getId());
              }
              collector.queryCompleted();
            } catch (AsyncApiException e) {
              LOG.error("Error closing job: {}", e);
            }
            LOG.info("Partial batch of {} records", numRecords);
            job = null;
            if (collector.subscribeToStreaming()) {
              // Switch to processing events
              nextSourceOffset = ForceUtils.READ_EVENTS_FROM_NOW;
            } else if (collector.repeatQuery() == ForceRepeatQuery.FULL) {
              nextSourceOffset = ForceUtils.RECORD_ID_OFFSET_PREFIX + collector.initialOffset();
            } else if (collector.repeatQuery() == ForceRepeatQuery.NO_REPEAT) {
              nextSourceOffset = null;
            }
          }
        }
      }
      LOG.info("Full batch of {} records", numRecords);
    }
    return nextSourceOffset;
  }

  public boolean queryInProgress() {
    return job != null;
  }

  private JobInfo createJob(String sobjectType, BulkConnection connection)
      throws AsyncApiException {
    BulkConnection bulkConnection = stage.getBulkConnection();
    ForceInputConfigBean conf = stage.getConfig();

    job = new JobInfo();
    job.setObject(sobjectType);
    job.setOperation((conf.queryAll && !conf.bulkConfig.usePKChunking) ? OperationEnum.queryAll : OperationEnum.query);
    job.setContentType(ContentType.XML);
    if (conf.bulkConfig.usePKChunking) {
      String headerValue = CHUNK_SIZE + "=" + conf.bulkConfig.chunkSize;
      if (!StringUtils.isEmpty(conf.bulkConfig.startId)) {
        headerValue += "; " + START_ROW + "=" + conf.bulkConfig.startId;
      }
      connection.addHeader(SFORCE_ENABLE_PKCHUNKING, headerValue);
    }
    try {
      job = connection.createJob(job);
    } catch (AsyncApiException e) {
      ForceUtils.renewSession(bulkConnection, e);
      job = connection.createJob(job);
    }
    return job;
  }

  public void destroy() {
    BulkConnection bulkConnection = stage.getBulkConnection();

    if (job != null) {
      try {
        try {
          bulkConnection.abortJob(job.getId());
        } catch (AsyncApiException e) {
          ForceUtils.renewSession(bulkConnection, e);
          bulkConnection.abortJob(job.getId());
        }
        job = null;
      } catch (AsyncApiException e) {
        LOG.error("Exception while aborting job", e);
      }
    }

    job = null;
  }
}
