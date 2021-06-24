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

package com.streamsets.pipeline.stage.cloudstorage.origin;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;
import com.google.common.collect.MinMaxPriorityQueue;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.event.NoMoreDataEvent;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.RecoverableDataParserException;
import com.streamsets.pipeline.lib.util.AntPathMatcher;
import com.streamsets.pipeline.stage.cloudstorage.lib.Errors;
import com.streamsets.pipeline.stage.cloudstorage.lib.GcsUtil;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class GoogleCloudStorageSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorageSource.class);
  private static final String OFFSET_DELIMITER = "::";
  private static final String START_FILE_OFFSET = "0";
  private static final String END_FILE_OFFSET = "-1";
  private static final String OFFSET_FORMAT = "%d"+ OFFSET_DELIMITER + "%s" + OFFSET_DELIMITER + "%s";
  private static final String BUCKET = "bucket";
  private static final String FILE = "file";
  private static final String SIZE = "size";

  private Storage storage;
  private GCSOriginConfig gcsOriginConfig;
  private DataParser parser;
  private AntPathMatcher antPathMatcher;
  private MinMaxPriorityQueue<Blob> minMaxPriorityQueue;
  private CredentialsProvider credentialsProvider;
  private ELEval rateLimitElEval;
  private ELVars rateLimitElVars;
  private long noMoreDataRecordCount;
  private long noMoreDataErrorCount;
  private long noMoreDataFileCount;
  private Blob blob = null;
  private GcsObjectPostProcessingHandler errorBlobHandler;
  private GcsObjectPostProcessingHandler postProcessingBlobHandler;
  private boolean checkBatchSize = true;

  GoogleCloudStorageSource(GCSOriginConfig gcsOriginConfig) {
    this.gcsOriginConfig = gcsOriginConfig;
  }

  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = gcsOriginConfig.init(getContext(), super.init());
    minMaxPriorityQueue = MinMaxPriorityQueue.orderedBy((Blob o1, Blob o2) -> {
      int result = o1.getUpdateTime().compareTo(o2.getUpdateTime());
      if(result != 0) {
        return result;
      }
      //same modified time. Use generatedid (bucket/blob name/timestamp) to sort
      return o1.getGeneratedId().compareTo(o2.getGeneratedId());
    }).maximumSize(gcsOriginConfig.maxResultQueueSize).create();
    antPathMatcher = new AntPathMatcher();

    gcsOriginConfig.credentials.getCredentialsProvider(getContext(), issues)
        .ifPresent(p -> credentialsProvider = p);

    try {
      storage = StorageOptions.newBuilder()
          .setCredentials(credentialsProvider.getCredentials())
          .build()
          .getService();
    } catch (IOException e) {
      LOG.error("Error when initializing storage. Reason : {}", e);
      issues.add(getContext().createConfigIssue(
          Groups.CREDENTIALS.name(),
          "gcsOriginConfig.credentials.credentialsProvider",
          Errors.GCS_01,
          e
      ));
    }

    rateLimitElEval = FileRefUtil.createElEvalForRateLimit(getContext());
    rateLimitElVars = getContext().createELVars();
    errorBlobHandler = new GcsObjectPostProcessingHandler(storage, gcsOriginConfig.gcsOriginErrorConfig.toPostProcessingConfig());
    postProcessingBlobHandler = new GcsObjectPostProcessingHandler(storage, gcsOriginConfig.postProcessingConfig);
    return issues;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    maxBatchSize = Math.min(maxBatchSize, gcsOriginConfig.basicConfig.maxBatchSize);
    if (checkBatchSize && gcsOriginConfig.basicConfig.maxBatchSize > maxBatchSize) {
      getContext().reportError(Errors.GCS_07, maxBatchSize);
      checkBatchSize = false;
    }

    long minTimestamp = 0;
    String blobGeneratedId = "";
    String fileOffset = START_FILE_OFFSET;

    if (!Strings.isNullOrEmpty(lastSourceOffset)) {
      minTimestamp = getMinTimestampFromOffset(lastSourceOffset);
      fileOffset = getFileOffsetFromOffset(lastSourceOffset);
      blobGeneratedId = getBlobIdFromOffset(lastSourceOffset);
    }

    if (minMaxPriorityQueue.isEmpty()) {
      poolForFiles(minTimestamp, blobGeneratedId, fileOffset);
    }
    // Process Blob
    if (parser == null) {
      //Get next eligible blob to read, if none throw no more data event
      do {
        if (blob != null) {
          postProcessingBlobHandler.handle(blob.getBlobId());
        }
        blob = minMaxPriorityQueue.pollFirst();
        //We don't have any spooled files to read from and we don't have anything available from the existing parser
        //(in case of sdc restart with stored offset, we still want some blob to be available for us to start reading)
        if (blob == null) {
          //No more data available
          if (noMoreDataRecordCount > 0 || noMoreDataErrorCount > 0) {
            LOG.info("sending no-more-data event.  records {} errors {} files {} ",
                noMoreDataRecordCount,
                noMoreDataErrorCount,
                noMoreDataFileCount
            );
            NoMoreDataEvent.EVENT_CREATOR.create(getContext())
                .with(NoMoreDataEvent.RECORD_COUNT, noMoreDataRecordCount)
                .with(NoMoreDataEvent.ERROR_COUNT, noMoreDataErrorCount)
                .with(NoMoreDataEvent.FILE_COUNT, noMoreDataFileCount)
                .createAndSend();
            noMoreDataRecordCount = 0;
            noMoreDataErrorCount = 0;
            noMoreDataFileCount = 0;
          }
          return lastSourceOffset;
        }
      } while (!isBlobEligible(blob, minTimestamp, blobGeneratedId, fileOffset));

      //If we are picking up from where we left off from previous offset
      //(i.e after sdc restart use the last offset, else use start file offset)
      fileOffset = (blobGeneratedId.equals(blob.getGeneratedId()))? fileOffset : START_FILE_OFFSET;
      blobGeneratedId = blob.getGeneratedId();

      if (gcsOriginConfig.dataFormat == DataFormat.WHOLE_FILE) {
        GCSFileRef.Builder gcsFileRefBuilder = new GCSFileRef.Builder()
            .bufferSize(gcsOriginConfig.dataParserFormatConfig.wholeFileMaxObjectLen)
            .createMetrics(true)
            .totalSizeInBytes(blob.getSize())
            .rateLimit(
                FileRefUtil.evaluateAndGetRateLimit(
                    rateLimitElEval,
                    rateLimitElVars, gcsOriginConfig.dataParserFormatConfig.rateLimit
                )
            )
            .blob(blob);

        if (gcsOriginConfig.dataParserFormatConfig.verifyChecksum) {
          gcsFileRefBuilder = gcsFileRefBuilder.verifyChecksum(true)
              .checksum(Hex.encodeHexString(Base64.getDecoder().decode(blob.getMd5())))
              .checksumAlgorithm(HashingUtil.HashType.MD5);
        }

        Map<String, Object> metadata = new HashMap<>();
        metadata.put(BUCKET, blob.getBucket());
        metadata.put(FILE, blob.getName());
        metadata.put(SIZE, blob.getSize());
        Optional.ofNullable(blob.getMetadata()).ifPresent(metadata::putAll);

        parser = gcsOriginConfig.dataParserFormatConfig.getParserFactory()
            .getParser(blobGeneratedId, metadata, gcsFileRefBuilder.build());
      } else {
        parser = gcsOriginConfig.dataParserFormatConfig.getParserFactory().getParser(
            blobGeneratedId,
            Channels.newInputStream(blob.reader()),
            fileOffset
        );
      }
      minTimestamp = blob.getUpdateTime();
    }

    try {
      int recordCount = 0;
      while (recordCount < maxBatchSize) {
        try {
          Record record = parser.parse();
          if (record != null) {
            batchMaker.addRecord(record);
            fileOffset = parser.getOffset();
            noMoreDataRecordCount++;
            recordCount++;
          } else {
            fileOffset = END_FILE_OFFSET;
            IOUtils.closeQuietly(parser);
            parser = null;
            noMoreDataFileCount++;
            break;
          }
        } catch (RecoverableDataParserException e) {
          LOG.error("Error when parsing record from object '{}' at offset '{}'. Reason : {}", blobGeneratedId, fileOffset, e);
          getContext().toError(e.getUnparsedRecord(), e.getErrorCode(), e.getParams());
        }
      }
    } catch (IOException | DataParserException e) {
      LOG.error("Error when parsing records from Object '{}'. Reason : {}, moving to next file", blobGeneratedId, e);
      getContext().reportError(Errors.GCS_00, blobGeneratedId, fileOffset ,e);
      fileOffset = END_FILE_OFFSET;
      noMoreDataErrorCount++;
      try {
        errorBlobHandler.handle(blob.getBlobId());
      } catch (StorageException se) {
        LOG.error("Error handling failed for {}. Reason{}", blobGeneratedId, e);
        getContext().reportError(Errors.GCS_06, blobGeneratedId, se);
      }
    }
    return String.format(OFFSET_FORMAT, minTimestamp, fileOffset, blobGeneratedId);
  }

  private void poolForFiles(long minTimeStamp, String currentBlobGeneratedId, String currentFileOffset) {
    Page<Blob> blobs = storage.list(
        gcsOriginConfig.bucketTemplate,
        Storage.BlobListOption.prefix(GcsUtil.normalizePrefix(gcsOriginConfig.commonPrefix))
    );
    blobs.iterateAll().forEach(blob ->  {
      if (isBlobEligible(blob, minTimeStamp, currentBlobGeneratedId, currentFileOffset)) {
        minMaxPriorityQueue.add(blob);
      }
    });
  }

  private boolean isBlobEligible(Blob blob, long minTimeStamp, String currentBlobGeneratedId, String currentFileOffset) {
    String blobName = blob.getName();
    String prefixToMatch =  blobName.substring(
        GcsUtil.normalizePrefix(gcsOriginConfig.commonPrefix).length(), blobName.length());
    return blob.getSize() > 0 &&
        //blob update time > current offset time
        (blob.getUpdateTime() > minTimeStamp
            //blob offset time = current offset time, but lexicographically greater than current offset
            || (blob.getUpdateTime() == minTimeStamp && blob.getGeneratedId().compareTo(currentBlobGeneratedId) > 0)
            //blob id same as current id and did not read till end of the file
            || blob.getGeneratedId().equals(currentBlobGeneratedId) && !END_FILE_OFFSET.equals(currentFileOffset))
        && antPathMatcher.match(gcsOriginConfig.prefixPattern, prefixToMatch);
  }

  private long getMinTimestampFromOffset(String offset) {
    return Long.valueOf(offset.split(OFFSET_DELIMITER, 3)[0]);
  }

  private String getFileOffsetFromOffset(String offset) {
    return offset.split(OFFSET_DELIMITER, 3)[1];
  }

  private String getBlobIdFromOffset(String offset) {
    return offset.split(OFFSET_DELIMITER, 3)[2];
  }

  @Override
  public void destroy() {
    IOUtils.closeQuietly(parser);
    blob = null;
  }
}
