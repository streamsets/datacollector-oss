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
package com.streamsets.pipeline.stage.destination.salesforce;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedCharDataGenerator;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.salesforce.Errors;
import com.streamsets.pipeline.lib.salesforce.ForceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
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
import java.util.TimeZone;

public class ForceBulkWriter extends ForceWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ForceBulkWriter.class);
  private static final int MAX_BYTES_PER_BATCH = 10000000; // 10 million bytes per batch
  private static final int MAX_ROWS_PER_BATCH = 10000; // 10 thousand rows per batch
  private static final String NA = "#N/A"; // Special value - sets field to null
  private final BulkConnection bulkConnection;
  private final Target.Context context;
  private Map<Integer, OperationEnum> opcodeToOperation = ImmutableMap.of(
      OperationType.INSERT_CODE, OperationEnum.insert,
      OperationType.DELETE_CODE, OperationEnum.delete,
      OperationType.UPDATE_CODE, OperationEnum.update,
      OperationType.UPSERT_CODE, OperationEnum.upsert
  );

  private static final TimeZone TZ = TimeZone.getTimeZone("GMT");
  private final SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'");
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
  private final SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

  class JobBatches {
    final JobInfo job;
    final List<BatchInfo> batchInfoList;
    final Collection<Record> records;

    JobBatches(JobInfo job, List<BatchInfo> batchInfoList, Collection<Record> records) {
      this.job = job;
      this.batchInfoList = batchInfoList;
      this.records = records;
    }
  }

  public ForceBulkWriter(
      PartnerConnection partnerConnection,
      String sObject,
      Map<String, String> customMappings,
      BulkConnection bulkConnection,
      Target.Context context
  ) throws ConnectionException {
    super(partnerConnection, sObject, customMappings);
    this.bulkConnection = bulkConnection;
    this.context = context;

    datetimeFormat.setTimeZone(TZ);
    dateFormat.setTimeZone(TZ);
    timeFormat.setTimeZone(TZ);
  }

  @Override
  List<OnRecordErrorException> writeBatch(
      String sObjectName,
      Collection<Record> records,
      ForceTarget target
  ) throws StageException {
    Iterator<Record> batchIterator = records.iterator();
    List<OnRecordErrorException> errorRecords = new LinkedList<>();
    Map<Integer, List<Record>> recordsByOp = new HashMap<>();

    // Build a list of records by operation code
    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();

      int opCode = ForceUtils.getOperationFromRecord(record, target.conf.defaultOperation,
          target.conf.unsupportedAction, errorRecords);

      recordsByOp.computeIfAbsent(opCode, k -> new ArrayList<>()).add(record);
    }

    List<JobBatches> jobs = new ArrayList<>(recordsByOp.size());

    // Do one (or more!) jobs per operation
    for (Map.Entry<Integer, List<Record>> entry : recordsByOp.entrySet()) {
      List<Record> recordList = entry.getValue();
      OperationEnum op = opcodeToOperation.get(entry.getKey());

      // Special handling - may be any number of External Id Fields
      if (op == OperationEnum.upsert) {
        // Partition by External Id Field
        Multimap<String, Record> partitions = ArrayListMultimap.create();

        for (Record record : recordList) {
          RecordEL.setRecordInContext(target.externalIdFieldVars, record);
          try {
            String partitionName = target.externalIdFieldEval.eval(target.externalIdFieldVars,
                target.conf.externalIdField, String.class);
            LOG.debug("Expression '{}' is evaluated to '{}' : ", target.conf.externalIdField, partitionName);
            partitions.put(partitionName, record);
          } catch (ELEvalException e) {
            LOG.error("Failed to evaluate expression '{}' : ", target.conf.externalIdField, e.toString(), e);
            errorRecords.add(new OnRecordErrorException(record, e.getErrorCode(), e.getParams()));
          }
        }

        for (String externalIdField : partitions.keySet()) {
          jobs.add(processJob(sObjectName, op, recordList, externalIdField));
        }
      } else {
        jobs.add(processJob(sObjectName, op, recordList, null));
      }
    }

    // Now wait for all the results
    for (JobBatches jb : jobs) {
      getJobResults(jb, errorRecords);
    }

    return errorRecords;
  }

  private JobBatches processJob(
      String sObjectName, OperationEnum operation, List<Record> records, String externalIdField
  ) throws
      StageException {
    try {
      JobInfo job = createJob(sObjectName, operation, externalIdField);
      List<BatchInfo> batchInfoList = createBatchesFromRecordCollection(job, records, operation);
      closeJob(job.getId());
      return new JobBatches(job, batchInfoList, records);
    } catch (AsyncApiException | IOException e) {
      throw new StageException(Errors.FORCE_13,
          ForceUtils.getExceptionCode(e) + ", " + ForceUtils.getExceptionMessage(e)
      );
    }
  }

  private void getJobResults(JobBatches jb, List<OnRecordErrorException> errorRecords) throws
      StageException {
    try {
      awaitCompletion(jb);
      checkResults(jb, errorRecords);
    } catch (AsyncApiException | IOException e) {
      throw new StageException(Errors.FORCE_13,
          ForceUtils.getExceptionCode(e) + ", " + ForceUtils.getExceptionMessage(e)
      );
    }
  }

  private JobInfo createJob(String sobjectType, OperationEnum operation, String externalIdField)
      throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setObject(sobjectType);
    job.setOperation(operation);
    if (externalIdField != null) {
      job.setExternalIdFieldName(externalIdField);
    }
    job.setContentType(ContentType.CSV);
    try {
      job = bulkConnection.createJob(job);
    } catch (AsyncApiException e) {
      ForceUtils.renewSession(bulkConnection, e);
      job = bulkConnection.createJob(job);
    }
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

  private List<BatchInfo> createBatchesFromRecordCollection(JobInfo jobInfo, Collection<Record> records, OperationEnum op)
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
      writeAndFlushRecord(gen, record, op);
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
        writeAndFlushRecord(gen, record, op);
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

  private void writeAndFlushRecord(DataGenerator gen, Record record, OperationEnum op) throws IOException, DataGeneratorException {
    // Make a record with just the fields we need
    Record outRecord = context.createRecord(record.getHeader().getSourceId());
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();

    for (Map.Entry<String, String> mapping : fieldMappings.entrySet()) {
      String sFieldName = mapping.getKey();
      String fieldPath = mapping.getValue();

      // If we're missing fields, skip them.
      if (!record.has(fieldPath)) {
        continue;
      }

      // We only need Id for deletes
      if (op == OperationEnum.delete && !("Id".equalsIgnoreCase(sFieldName))) {
        continue;
      }


      Field field = record.get(fieldPath);
      if (field.getValue() == null &&
          (op == OperationEnum.update || op == OperationEnum.upsert)) {
        field = Field.create(NA);
      }

      switch (field.getType()) {
        case DATE:
          map.put(sFieldName, Field.create(dateFormat.format(field.getValue())));
          break;
        case TIME:
          map.put(sFieldName, Field.create(timeFormat.format(field.getValue())));
          break;
        case DATETIME:
          map.put(sFieldName, Field.create(datetimeFormat.format(field.getValue())));
          break;
        default:
          map.put(sFieldName, field);
          break;
      }
    }

    outRecord.set(Field.createListMap(map));
    gen.write(outRecord);
    gen.flush();
  }

  private void createBatch(byte[] bytes, int count, List<BatchInfo> batchInfos, JobInfo jobInfo)
      throws AsyncApiException {
    try {
      createBatchInternal(bytes, count, batchInfos, jobInfo);
    } catch (AsyncApiException e) {
      ForceUtils.renewSession(bulkConnection, e);
      createBatchInternal(bytes, count, batchInfos, jobInfo);
    }
  }

  private void createBatchInternal(byte[] bytes, int count, List<BatchInfo> batchInfos, JobInfo jobInfo) throws
      AsyncApiException {
    BatchInfo batchInfo;
    batchInfo = bulkConnection.createBatchFromStream(jobInfo, new ByteArrayInputStream(bytes, 0, count));
    LOG.info("Wrote Bulk API batch: {}", batchInfo);
    batchInfos.add(batchInfo);
  }

  private void closeJob(String jobId)
      throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setId(jobId);
    job.setState(JobStateEnum.Closed);
    try {
      bulkConnection.updateJob(job);
    } catch (AsyncApiException e) {
      ForceUtils.renewSession(bulkConnection, e);
      bulkConnection.updateJob(job);
    }
  }

  private void awaitCompletion(JobBatches jb)
      throws AsyncApiException {
    long sleepTime = 0L;
    Set<String> incomplete = new HashSet<>();
    for (BatchInfo bi : jb.batchInfoList) {
      incomplete.add(bi.getId());
    }
    while (!incomplete.isEmpty()) {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {}
      LOG.info("Awaiting Bulk API results... {}", incomplete.size());
      sleepTime = 1000L;
      BatchInfo[] statusList = null;
      try {
        statusList = bulkConnection.getBatchInfoList(jb.job.getId()).getBatchInfo();
      } catch (AsyncApiException e) {
        ForceUtils.renewSession(bulkConnection, e);
        statusList = bulkConnection.getBatchInfoList(jb.job.getId()).getBatchInfo();
      }
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
      JobBatches jb, List<OnRecordErrorException> errorRecords
  ) throws AsyncApiException, IOException {
    Record[] recordArray = jb.records.toArray(new Record[0]);
    int recordIndex = 0;
    for (BatchInfo b : jb.batchInfoList) {
      CSVReader rdr = null;
      try {
        rdr = new CSVReader(bulkConnection.getBatchResultStream(jb.job.getId(), b.getId()));
      } catch (AsyncApiException e) {
        ForceUtils.renewSession(bulkConnection, e);
        rdr = new CSVReader(bulkConnection.getBatchResultStream(jb.job.getId(), b.getId()));
      }
      List<String> resultHeader = rdr.nextRecord();
      int resultCols = resultHeader.size();

      List<String> row;
      while ((row = rdr.nextRecord()) != null) {
        Map<String, String> resultInfo = new HashMap<>();
        for (int i = 0; i < resultCols; i++) {
          resultInfo.put(resultHeader.get(i), row.get(i));
        }
        boolean success = Boolean.parseBoolean(resultInfo.get("Success"));
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
