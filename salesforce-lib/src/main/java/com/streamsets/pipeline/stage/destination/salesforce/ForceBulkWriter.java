/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.salesforce;

import com.google.common.collect.Sets;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedCharDataGenerator;
import com.streamsets.pipeline.lib.salesforce.Errors;
import com.streamsets.pipeline.lib.salesforce.ForceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

public class ForceBulkWriter extends ForceWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ForceBulkWriter.class);
  private static final int MAX_BYTES_PER_BATCH = 10000000; // 10 million bytes per batch
  private static final int MAX_ROWS_PER_BATCH = 10000; // 10 thousand rows per batch
  private final BulkConnection bulkConnection;
  private final Target.Context context;

  public ForceBulkWriter(Map<String, String> fieldMappings, BulkConnection bulkConnection, Target.Context context) {
    super(fieldMappings);
    this.bulkConnection = bulkConnection;
    this.context = context;
  }

  @Override
  public List<OnRecordErrorException> writeBatch(
      String sObjectName,
      Collection<Record> records
  ) throws StageException {
    List<OnRecordErrorException> errorRecords = new LinkedList<>();

    try {
      JobInfo job = createJob(sObjectName);
      List<BatchInfo> batchInfoList = createBatchesFromRecordCollection(job, records);
      closeJob(job.getId());
      awaitCompletion(job, batchInfoList);
      checkResults(job, batchInfoList, records, errorRecords);
    } catch (AsyncApiException | IOException e) {
      throw new StageException(Errors.FORCE_13, ForceUtils.getExceptionCode(e) + ", " + ForceUtils.getExceptionMessage(e));
    }

    return errorRecords;
  }

  private JobInfo createJob(String sobjectType)
      throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setObject(sobjectType);
    job.setOperation(OperationEnum.insert);
    job.setContentType(ContentType.CSV);
    job = bulkConnection.createJob(job);
    LOG.info("Created Bulk API job {}", job.getId());
    return job;
  }

  private DataGenerator createDelimitedCharDataGenerator(OutputStreamWriter writer) throws StageException {
    try {
      return new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.WITH_HEADER, "header", "value", null);
    } catch (IOException ioe) {
      throw new StageException(Errors.FORCE_14, ioe);
    }
  }

  private List<BatchInfo> createBatchesFromRecordCollection(JobInfo jobInfo, Collection<Record> records)
      throws IOException, AsyncApiException, StageException {
    List<BatchInfo> batchInfos = new ArrayList<>();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(baos, "UTF-8" );

    DataGenerator gen = createDelimitedCharDataGenerator(writer);

    Iterator<Record> batchIterator = records.iterator();

    int lastBytes = 0;
    int currentBytes = 0;
    int currentLines = 1; // Count header line

    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      writeAndFlushRecord(gen, record);
      currentBytes = baos.size();

      // Create a new batch when our batch size limit is reached
      if (currentBytes > MAX_BYTES_PER_BATCH
          || currentLines > MAX_ROWS_PER_BATCH) {
        byte[] bytes = baos.toByteArray();
        createBatch(bytes, lastBytes, batchInfos, jobInfo);

        baos.reset();
        gen.close();
        // Get a new writer so we get a new header line
        gen = createDelimitedCharDataGenerator(writer);

        // Rewrite the record in hand, since we chopped it off the end of the buffer
        writeAndFlushRecord(gen, record);
        currentBytes = baos.size();
        currentLines = 1;
      }

      lastBytes = currentBytes;
      currentLines++;
    }

    // Finished processing all rows
    // Create a final batch for any remaining data
    if (currentLines > 1) {
      byte[] bytes = baos.toByteArray();
      createBatch(bytes, bytes.length, batchInfos, jobInfo);
    }

    gen.close();

    return batchInfos;
  }

  private void writeAndFlushRecord(DataGenerator gen, Record record) throws IOException, DataGeneratorException {
    // Make a record with just the fields we need
    Record outRecord = context.createRecord(record.getHeader().getSourceId());
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();

    SortedSet<String> columnsPresent = Sets.newTreeSet(fieldMappings.keySet());
    for (Map.Entry<String, String> mapping : fieldMappings.entrySet()) {
      String sFieldName = mapping.getKey();
      String fieldPath = mapping.getValue();

      // If we're missing fields, skip them.
      if (!record.has(fieldPath)) {
        columnsPresent.remove(sFieldName);
        continue;
      }

      map.put(sFieldName, record.get(fieldPath));
    }

    outRecord.set(Field.createListMap(map));
    gen.write(outRecord);
    gen.flush();
  }

  private void createBatch(byte[] bytes, int count, List<BatchInfo> batchInfos, JobInfo jobInfo)
      throws IOException, AsyncApiException {
    BatchInfo batchInfo =
        bulkConnection.createBatchFromStream(jobInfo, new ByteArrayInputStream(bytes, 0, count));
    LOG.info("Wrote Bulk API batch: {}", batchInfo);
    batchInfos.add(batchInfo);
  }

  private void closeJob(String jobId)
      throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setId(jobId);
    job.setState(JobStateEnum.Closed);
    bulkConnection.updateJob(job);
  }

  private void awaitCompletion(JobInfo job,
      List<BatchInfo> batchInfoList)
      throws AsyncApiException {
    long sleepTime = 0L;
    Set<String> incomplete = new HashSet<>();
    for (BatchInfo bi : batchInfoList) {
      incomplete.add(bi.getId());
    }
    while (!incomplete.isEmpty()) {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {}
      LOG.info("Awaiting Bulk API results... {}", incomplete.size());
      sleepTime = 1000L;
      BatchInfo[] statusList =
          bulkConnection.getBatchInfoList(job.getId()).getBatchInfo();
      for (BatchInfo b : statusList) {
        if (b.getState() == BatchStateEnum.Completed
            || b.getState() == BatchStateEnum.Failed) {
          if (incomplete.remove(b.getId())) {
            LOG.info("Batch status: {}", b);
          }
        }
      }
    }
  }

  private void checkResults(
      JobInfo job, List<BatchInfo> batchInfoList, Collection<Record> records, List<OnRecordErrorException> errorRecords
  ) throws AsyncApiException, IOException {
    Record[] recordArray = records.toArray(new Record[0]);
    int recordIndex = 0;
    for (BatchInfo b : batchInfoList) {
      CSVReader rdr =
          new CSVReader(bulkConnection.getBatchResultStream(job.getId(), b.getId()));
      List<String> resultHeader = rdr.nextRecord();
      int resultCols = resultHeader.size();

      List<String> row;
      while ((row = rdr.nextRecord()) != null) {
        Map<String, String> resultInfo = new HashMap<>();
        for (int i = 0; i < resultCols; i++) {
          resultInfo.put(resultHeader.get(i), row.get(i));
        }
        boolean success = Boolean.valueOf(resultInfo.get("Success"));
        String error = resultInfo.get("Error");
        if (!success) {
          Record record = recordArray[recordIndex];
          errorRecords.add(new OnRecordErrorException(
              record,
              Errors.FORCE_11,
              record.getHeader().getSourceId(),
              error
          ));
        }
        recordIndex++;
      }
    }
  }
}
