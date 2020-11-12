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
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.AbortedException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.ext.io.ObjectLengthException;
import com.streamsets.pipeline.api.ext.io.OverrunException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.api.service.dataformats.DataParser;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import com.streamsets.pipeline.api.service.dataformats.RecoverableDataParserException;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AmazonS3Runnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(AmazonS3Runnable.class);

  private PushSource.Context context;
  private int runnerId;
  private int batchSize;
  private S3ConfigBean s3ConfigBean;
  private S3Spooler spooler;
  private AmazonS3Source amazonS3Source;

  private AmazonS3 s3Client;

  private DataParser parser;
  private S3Object object;
  private DataFormatParserService dataParser;
  private ErrorRecordHandler errorRecordHandler;

  private final Map<String, Object> gaugeMap;

  private S3ObjectSummary currentObject;

  public AmazonS3Runnable(
      PushSource.Context context,
      int batchSize,
      int threadNumber,
      S3ConfigBean s3ConfigBean,
      S3Spooler spooler,
      AmazonS3Source amazonS3Source
  ) {
    this.context = context;
    this.runnerId = threadNumber;
    this.batchSize = batchSize;
    this.s3ConfigBean = s3ConfigBean;
    this.spooler = spooler;
    this.amazonS3Source = amazonS3Source;

    // Metrics
    this.gaugeMap = context.createGauge(S3Constants.AMAZON_S3_METRICS + runnerId).getValue();
  }

  @Override
  public void run() {
    String oldThreadName = Thread.currentThread().getName();
    Thread.currentThread().setName(S3Constants.AMAZON_S3_THREAD_PREFIX + runnerId);
    try {
      s3Client = s3ConfigBean.s3Config.getS3Client();
      initGaugeIfNeeded();
      S3Offset offset;
      while (!context.isStopped()) {
        offset = amazonS3Source.getOffset(runnerId);
        BatchContext batchContext = context.startBatch();
        this.errorRecordHandler = new DefaultErrorRecordHandler(context, batchContext);
        this.dataParser = context.getService(DataFormatParserService.class);
        dataParser.setStringBuilderPoolSize(s3ConfigBean.numberOfThreads);
        try {
          amazonS3Source.updateOffset(runnerId, produce(offset, batchSize, batchContext));
        } catch (BadSpoolObjectException ex) {
          LOG.error(Errors.S3_SPOOLDIR_01.getMessage(), ex.getObject(), ex.getPos(), ex.toString(), ex);
          context.reportError(Errors.S3_SPOOLDIR_01, ex.getObject(), ex.getPos(), ex.toString());
          try {
            spooler.handleCurrentObjectAsError();
          } catch (AmazonClientException e) {
            handleStageError(Errors.S3_SPOOLDIR_24, e);
          }
          // we set the offset to -1 to indicate we are done with the current object and we should fetch a new one
          // from the spooler
          amazonS3Source.updateOffset(runnerId, new S3Offset(offset, S3Constants.MINUS_ONE));
        }
      }
    } catch (StageException e) {
      handleStageError(e.getErrorCode(), e);
    } finally {
      Thread.currentThread().setName(oldThreadName);
      IOUtils.closeQuietly(parser);
    }
  }

  private S3Offset produce(S3Offset offset, int maxBatchSize, BatchContext batchContext)
      throws StageException, BadSpoolObjectException {
    BatchMaker batchMaker = batchContext.getBatchMaker();

    S3ObjectSummary s3Object;

    if (offset != null) {
      spooler.postProcessOlderObjectIfNeeded(offset);
    }

    //check if we have an object to produce records from. Otherwise get from spooler.
    if (needToFetchNextObjectFromSpooler(offset)) {
      updateGauge(Status.SPOOLING, null);
      offset = fetchNextObjectFromSpooler(offset, batchContext);
      LOG.debug("Object '{}' with offset '{}' fetched from Spooler", offset.getKey(), offset.getOffset());
      if (getCurrentObject() != null) {
        amazonS3Source.sendNewFileEvent(offset.getKey(), batchContext);
      }
    } else {
      //check if the current object was modified between batches
      LOG.debug("Checking if Object '{}' has been modified between batches", getCurrentObject().getKey());
      if (!getCurrentObject().getETag().equals(offset.geteTag())) {
        //send the current object to error archive and get next object from spooler
        LOG.debug("Object '{}' has been modified between batches. Sending the object to error",
            getCurrentObject().getKey()
        );
        try {
          spooler.handleCurrentObjectAsError();
        } catch (AmazonClientException e) {
          throw new StageException(Errors.S3_SPOOLDIR_24, e.toString(), e);
        }
        offset = fetchNextObjectFromSpooler(offset, batchContext);
      }
    }

    s3Object = getCurrentObject();
    if (s3Object != null) {
      amazonS3Source.updateOffset(runnerId, offset);
      try {
        if (parser == null) {
          String recordId = s3ConfigBean.s3Config.bucket + s3ConfigBean.s3Config.delimiter + s3Object.getKey();
          if (dataParser.isWholeFileFormat()) {
            handleWholeFileDataFormat(s3Object, recordId);
          } else {
            //Get S3 object instead of stream because we want to call close on the object when we close the
            // parser (and stream)
            if (context.isPreview()) {
              long fetchSize = s3Object.getSize() > S3Constants.DEFAULT_FETCH_SIZE
                               ? S3Constants.DEFAULT_FETCH_SIZE
                               : s3Object.getSize();
              if (fetchSize > 0) {
                object = AmazonS3Util.getObjectRange(s3Client,
                    s3ConfigBean.s3Config.bucket,
                    s3Object.getKey(),
                    fetchSize,
                    s3ConfigBean.sseConfig.useCustomerSSEKey,
                    s3ConfigBean.sseConfig.customerKey,
                    s3ConfigBean.sseConfig.customerKeyMd5
                );
              } else {
                LOG.warn("Size of object with key '{}' is 0", s3Object.getKey());
                object = AmazonS3Util.getObject(s3Client,
                    s3ConfigBean.s3Config.bucket,
                    s3Object.getKey(),
                    s3ConfigBean.sseConfig.useCustomerSSEKey,
                    s3ConfigBean.sseConfig.customerKey,
                    s3ConfigBean.sseConfig.customerKeyMd5
                );
              }
            } else {
              object = AmazonS3Util.getObject(s3Client,
                  s3ConfigBean.s3Config.bucket,
                  s3Object.getKey(),
                  s3ConfigBean.sseConfig.useCustomerSSEKey,
                  s3ConfigBean.sseConfig.customerKey,
                  s3ConfigBean.sseConfig.customerKeyMd5
              );
            }
            parser = dataParser.getParser(recordId, object.getObjectContent(), offset.getOffset());
          }
          sendLineageEvent(s3Object);
          //we don't use S3 GetObject range capabilities to skip the already process offset because the parsers cannot
          // pick up from a non root doc depth in the case of a single object with records.
        }
        int i = 0;
        updateGauge(Status.READING, offset.toString());
        while (i < maxBatchSize) {
          try {
            Record record;
            try {
              record = parser.parse();
            } catch (RecoverableDataParserException ex) {
              // Propagate partially parsed record to error stream
              record = ex.getUnparsedRecord();
              setHeaders(record, object);
              errorRecordHandler.onError(new OnRecordErrorException(record, ex.getErrorCode(), ex.getParams()));
              // We'll simply continue reading pass this recoverable error
              continue;
            }
            if (record != null) {
              setHeaders(record, object);
              batchMaker.addRecord(record);
              amazonS3Source.incrementNoMoreDataRecordCount();
              amazonS3Source.incrementFileFinishedRecordCounter(offset.getKey());
              i++;
              offset = new S3Offset(offset, parser.getOffset());
            } else {
              parser.close();
              parser = null;
              if (object != null) {
                object.close();
                object = null;
              }
              amazonS3Source.incrementNoMoreDataFileCount();
              offset = new S3Offset(offset, S3Constants.MINUS_ONE);
              amazonS3Source.sendFileFinishedEvent(offset.getKey(), batchContext);
              break;
            }
          } catch (ObjectLengthException ex) {
            errorRecordHandler.onError(Errors.S3_SPOOLDIR_02, s3Object.getKey(), offset.toString(), ex);
            amazonS3Source.incrementNoMoreDataErrorCount();
            amazonS3Source.incrementFileFinishedErrorCounter(offset.getKey());
            offset = new S3Offset(offset, S3Constants.MINUS_ONE);
          }
        }
      } catch (AmazonClientException e) {
        LOG.error("Error processing object with key '{}' offset '{}'", s3Object.getKey(), offset, e);
        throw new StageException(Errors.S3_SPOOLDIR_25, e.toString(), e);
      } catch (IOException | DataParserException ex) {
        if (!(ex.getCause() instanceof AbortedException)) {
          offset = new S3Offset(offset, S3Constants.MINUS_ONE);
          String exOffset;
          if (ex instanceof OverrunException) {
            exOffset = String.valueOf(((OverrunException) ex).getStreamOffset());
          } else {
            try {
              exOffset = (parser != null) ? parser.getOffset() : S3Constants.MINUS_ONE;
            } catch (IOException ex1) {
              LOG.warn("Could not get the object offset to report with error, reason: {}", ex1.toString(), ex);
              exOffset = S3Constants.MINUS_ONE;
            }
          }

          switch (context.getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              // we failed to produce a record, which leaves the input file in an unknown state. all we can do here is
              // throw an exception.
              throw new BadSpoolObjectException(s3Object.getKey(), exOffset, ex);
            case STOP_PIPELINE:
              context.reportError(Errors.S3_SPOOLDIR_03, s3Object.getKey(), exOffset, ex.toString(), ex);
              throw new StageException(Errors.S3_SPOOLDIR_03, s3Object.getKey(), exOffset, ex.toString(), ex);
            default:
              throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
                  context.getOnErrorRecord(),
                  ex
              ));
          }
        }
      } finally {
        if (S3Constants.MINUS_ONE.equals(offset.getOffset())) {
          if (parser != null) {
            try {
              parser.close();
              parser = null;
            } catch (IOException ex) {
              LOG.debug("Exception while closing parser : '{}'", ex.toString(), ex);
            }
          }
          if (object != null) {
            try {
              object.close();
              object = null;
            } catch (IOException ex) {
              LOG.debug("Exception while closing S3 object : '{}'", ex.toString(), ex);
            }
          }
        }
      }
    }
    if (!context.processBatch(batchContext) && context.getDeliveryGuarantee().equals(DeliveryGuarantee.AT_LEAST_ONCE)) {
      return null;
    }
    updateGauge(Status.BATCH_GENERATED, offset.toString());
    return offset;
  }

  //For whole file we do not care whether it is a preview or not,
  //as the record is just the metadata along with file ref.
  private void handleWholeFileDataFormat(S3ObjectSummary s3ObjectSummary, String recordId) throws StageException {
    S3Object partialS3ObjectForMetadata;
    //partialObject with fetchSize 1 byte.
    //This is mostly used for extracting metadata and such.
    if(s3ObjectSummary.getSize() == 0) {
      partialS3ObjectForMetadata = AmazonS3Util.getObject(
          s3Client,
          s3ConfigBean.s3Config.bucket,
          s3ObjectSummary.getKey(),
          s3ConfigBean.sseConfig.useCustomerSSEKey,
          s3ConfigBean.sseConfig.customerKey,
          s3ConfigBean.sseConfig.customerKeyMd5
      );
    } else {
      partialS3ObjectForMetadata = AmazonS3Util.getObjectRange(
          s3Client,
          s3ConfigBean.s3Config.bucket,
          s3ObjectSummary.getKey(),
          1,
          s3ConfigBean.sseConfig.useCustomerSSEKey,
          s3ConfigBean.sseConfig.customerKey,
          s3ConfigBean.sseConfig.customerKeyMd5
      );
    }

    S3FileRef.Builder s3FileRefBuilder = new S3FileRef.Builder().s3Client(s3Client)
                                                                .s3ObjectSummary(s3ObjectSummary)
                                                                .useSSE(s3ConfigBean.sseConfig.useCustomerSSEKey)
                                                                .customerKey(s3ConfigBean.sseConfig.customerKey)
                                                                .customerKeyMd5(s3ConfigBean.sseConfig.customerKeyMd5)
                                                                .bufferSize((int) dataParser.suggestedWholeFileBufferSize())
                                                                .createMetrics(true)
                                                                .totalSizeInBytes(s3ObjectSummary.getSize())
                                                                .rateLimit(dataParser.wholeFileRateLimit());

    if (dataParser.isWholeFileChecksumRequired()) {
      s3FileRefBuilder.verifyChecksum(true).checksumAlgorithm(HashingUtil.HashType.MD5)
                      //128 bit hex encoded md5 checksum.
                      .checksum(partialS3ObjectForMetadata.getObjectMetadata().getETag());
    }
    Map<String, Object> metadata = AmazonS3Util.getMetaData(partialS3ObjectForMetadata);
    metadata.put(S3Constants.BUCKET, s3ObjectSummary.getBucketName());
    metadata.put(S3Constants.OBJECT_KEY, s3ObjectSummary.getKey());
    metadata.put(S3Constants.OWNER, s3ObjectSummary.getOwner());
    metadata.put(S3Constants.SIZE, s3ObjectSummary.getSize());
    metadata.put(HeaderAttributeConstants.FILE_NAME, s3ObjectSummary.getKey());

    metadata.remove(S3Constants.CONTENT_LENGTH);
    parser = dataParser.getParser(recordId, metadata, s3FileRefBuilder.build());
    //Object is assigned so that setHeaders() function can use this to get metadata
    //information about the object
    object = partialS3ObjectForMetadata;
  }

  private boolean needToFetchNextObjectFromSpooler(S3Offset s3Offset) {
    return
        // we don't have an object half way processed in the current agent execution
        getCurrentObject() == null ||
        // we don't have an object half way processed from a previous agent execution via offset tracking
        s3Offset.getKey() == null ||
        s3Offset.getKey().equals(S3Constants.EMPTY) ||
        // the current object has been fully processed
        S3Constants.MINUS_ONE.equals(s3Offset.getOffset());
  }

  private S3ObjectSummary getCurrentObject() {
    return currentObject;
  }

  private void setCurrentObject(S3ObjectSummary currentObject) {
    this.currentObject = currentObject;
  }

  private S3Offset fetchNextObjectFromSpooler(S3Offset s3Offset, BatchContext batchContext) throws StageException {
    setCurrentObject(null);
    try {
      //The next object found in queue is mostly eligible since we process objects in chronological order.

      //However after processing a few files, if the configuration is changed [say relax the prefix] and an older file
      //gets selected for processing, it must be ignored.
      S3ObjectSummary nextAvailObj = null;
      do {
        if (nextAvailObj != null) {
          LOG.warn("Ignoring object '{}' in spool directory as is lesser than offset object '{}'",
              nextAvailObj.getKey(),
              s3Offset.getKey()
          );
        }
        nextAvailObj = spooler.poolForObject(amazonS3Source,
            s3ConfigBean.basicConfig.maxWaitTime,
            TimeUnit.MILLISECONDS,
            batchContext
        );
      } while (!isEligible(nextAvailObj, s3Offset));

      if (nextAvailObj == null) {
        // no object to process
        LOG.debug("No new object available in spool queue after '{}' secs, producing empty batch",
            s3ConfigBean.basicConfig.maxWaitTime / 1000
        );
      } else {
        setCurrentObject(nextAvailObj);

        // if the current offset object is null or the object returned by the spooler is greater than the current offset
        // object we take the object returned by the spooler as the new object and set the offset to zero.
        // if not, it means the spooler returned us the current object, we just keep processing it from the last
        // offset we processed (known via offset tracking)
        if (s3Offset.getKey() == null ||
            s3Offset.getKey().equals(S3Constants.EMPTY) ||
            isLaterThan(nextAvailObj.getKey(),
                nextAvailObj.getLastModified().getTime(),
                s3Offset.getKey(),
                Long.parseLong(s3Offset.getTimestamp())
            )) {
          s3Offset = new S3Offset(getCurrentObject().getKey(),
              S3Constants.ZERO,
              getCurrentObject().getETag(),
              String.valueOf(getCurrentObject().getLastModified().getTime())
          );
        }
      }
    } catch (InterruptedException ex) {
      // the spooler was interrupted while waiting for an object, we log and return, the pipeline agent will invoke us
      // again to wait for an object again
      LOG.warn("Pooling interrupted");
    } catch (AmazonClientException e) {
      throw new StageException(Errors.S3_SPOOLDIR_23, e.toString(), e);
    }
    return s3Offset;
  }

  private boolean isLaterThan(String nextKey, long nextTimeStamp, String originalKey, long originalTimestamp) {
    ObjectOrdering objectOrdering = s3ConfigBean.s3FileConfig.objectOrdering;
    switch (objectOrdering) {
      case TIMESTAMP:
        return (nextTimeStamp > originalTimestamp) || (
            nextTimeStamp == originalTimestamp && nextKey.compareTo(originalKey) > 0
        );
      case LEXICOGRAPHICAL:
        return nextKey.compareTo(originalKey) > 0;
      default:
        throw new IllegalArgumentException("Unknown ordering: " + objectOrdering.getLabel());
    }
  }

  private boolean isEligible(S3ObjectSummary nextAvailObj, S3Offset s3Offset) {

    ObjectOrdering objectOrdering = s3ConfigBean.s3FileConfig.objectOrdering;
    switch (objectOrdering) {
      case TIMESTAMP:
        return nextAvailObj == null || s3Offset == null || nextAvailObj.getLastModified().getTime() >= Long.parseLong(
            s3Offset.getTimestamp());
      case LEXICOGRAPHICAL:
        return nextAvailObj == null || s3Offset == null || s3Offset.getKey() == null
            || s3Offset.getKey().equals(S3Constants.EMPTY) || nextAvailObj.getKey().compareTo(s3Offset.getKey()) > 0
            || (nextAvailObj.getKey().compareTo(s3Offset.getKey()) == 0 && AmazonS3Util.parseOffset(s3Offset) != -1);
      default:
        throw new IllegalArgumentException("Unknown ordering: " + objectOrdering.getLabel());
    }
  }

  private void sendLineageEvent(S3ObjectSummary s3Object) {
    LineageEvent event = context.createLineageEvent(LineageEventType.ENTITY_READ);
    event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.S3.name());
    event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME, s3Object.getKey());
    event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, s3ConfigBean.s3Config.bucket);

    context.publishLineageEvent(event);
  }

  private void setHeaders(Record record, S3Object object) {
    if (s3ConfigBean.enableMetaData) {
      // if metadata is enabled, set the metadata to the header
      Map<String, Object> metaData = AmazonS3Util.getMetaData(object);
      for (Map.Entry<String, Object> entry : metaData.entrySet()) {
        //Content-Length is partial for whole file format, so not populating it here
        //Users can always look at /record/fileInfo/size to get the real size.
        boolean shouldAddThisMetadata = !(
            dataParser.isWholeFileFormat() && entry.getKey().equals(S3Constants.CONTENT_LENGTH)
        );
        if (shouldAddThisMetadata) {
          String value = entry.getValue() == null ? "" : entry.getValue().toString();
          record.getHeader().setAttribute(entry.getKey(), value);
        }
      }
      // set file name to the header
      record.getHeader().setAttribute("Name", object.getKey());
    }
  }

  private enum Status {
    SPOOLING,
    READING,
    BATCH_GENERATED,
    ;
  }

  /**
   * Initialize the gauge with needed information
   */
  private void initGaugeIfNeeded() {
    gaugeMap.put(S3Constants.THREAD_NAME, Thread.currentThread().getName());
    gaugeMap.put(S3Constants.STATUS, "");
    gaugeMap.put(S3Constants.BUCKET, "");
    gaugeMap.put(S3Constants.OBJECT_KEY, "");
  }

  private void updateGauge(Status status, String offset) {
    gaugeMap.put(S3Constants.STATUS, status.name());
    gaugeMap.put(S3Constants.OBJECT_KEY, currentObject == null ? "" : currentObject.getKey());
    gaugeMap.put(S3Constants.BUCKET, currentObject == null ? "" : currentObject.getBucketName());
    gaugeMap.put(S3Constants.OFFSET, offset == null ? "" : offset);
  }

  /**
   * Handle Exception
   */
  private void handleStageError(ErrorCode errorCode, Exception e) {
    final String errorMessage = "A failure occurred";
    LOG.error(errorMessage, e);
    try {
      errorRecordHandler.onError(errorCode, e);
    } catch (StageException se) {
      LOG.error("Error when routing to stage error", se);
      //Way to throw stage exception from runnable to main source thread
      Throwables.propagate(se);
    }
  }
}
