/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class S3Spooler {

  private static final Logger LOG = LoggerFactory.getLogger(S3Spooler.class);

  private final Source.Context context;
  private final S3FileConfig s3FileConfig;
  private final S3Config config;
  private final S3PostProcessingConfig postProcessingConfig;
  private final S3ErrorConfig errorConfig;
  private final AmazonS3Client s3Client;

  public S3Spooler(Source.Context context, S3FileConfig s3FileConfig, S3Config config,
                   S3PostProcessingConfig postProcessingConfig, S3ErrorConfig errorConfig, AmazonS3Client s3Client) {
    this.context = context;
    this.s3FileConfig = s3FileConfig;
    this.config = config;
    this.postProcessingConfig = postProcessingConfig;
    this.errorConfig = errorConfig;
    this.s3Client = s3Client;
  }

  private S3ObjectSummary currentObject;
  private ArrayBlockingQueue<S3ObjectSummary> objectQueue;
  private Meter spoolQueueMeter;

  public void init() {
    try {
      objectQueue = new ArrayBlockingQueue<>(s3FileConfig.maxSpoolObjects);
      spoolQueueMeter = context.createMeter("spoolQueue");
      S3ObjectSummary lastFound = findAndQueueObjects(false);
      if(lastFound != null) {
        LOG.debug("Last file found '{}' on startup", lastFound.getKey());
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void destroy() {
    if(objectQueue != null) {
      objectQueue.clear();
      objectQueue = null;
    }
  }

  S3ObjectSummary findAndQueueObjects(boolean checkCurrent) throws AmazonClientException {
    List<S3ObjectSummary> s3ObjectSummaries = AmazonS3Util.listObjectsChronologically(s3Client, config,
      objectQueue.remainingCapacity());
    for (S3ObjectSummary objectSummary : s3ObjectSummaries) {
      addObjectToQueue(objectSummary, checkCurrent);
    }
    spoolQueueMeter.mark(objectQueue.size());
    LOG.debug("Found '{}' files", objectQueue.size());
    return (s3ObjectSummaries.isEmpty()) ? null : s3ObjectSummaries.get(s3ObjectSummaries.size() - 1);
  }

  void addObjectToQueue(S3ObjectSummary objectSummary, boolean checkCurrent) {
    Preconditions.checkNotNull(objectSummary, "file cannot be null");
    if (checkCurrent) {
      Preconditions.checkState(currentObject == null ||
        currentObject.getLastModified().compareTo(objectSummary.getLastModified()) < 0);
    }
    if (!objectQueue.contains(objectSummary)) {
      if (objectQueue.size() >= s3FileConfig.maxSpoolObjects) {
        throw new IllegalStateException(Utils.format("Exceeded max number '{}' of queued files",
          s3FileConfig.maxSpoolObjects));
      }
      objectQueue.add(objectSummary);
      spoolQueueMeter.mark(objectQueue.size());
    } else {
      LOG.warn("Object '{}' already in queue, ignoring", objectSummary.getKey());
    }
  }

  public S3ObjectSummary poolForObject(long wait, TimeUnit timeUnit) throws InterruptedException, AmazonClientException {
    Preconditions.checkArgument(wait >= 0, "wait must be zero or greater");
    Preconditions.checkNotNull(timeUnit, "timeUnit cannot be null");

    if(objectQueue.size() == 0) {
      findAndQueueObjects(false);
    }

    S3ObjectSummary next = null;
    try {
      LOG.debug("Polling for file, waiting '{}' ms", TimeUnit.MILLISECONDS.convert(wait, timeUnit));
      next = objectQueue.poll(wait, timeUnit);
    } catch (InterruptedException ex) {
      next = null;
    } finally {
      LOG.debug("Polling for file returned '{}'", next);
      if (next != null) {
        currentObject = next;
      }
    }
    return next;
  }

  void postProcess(String postProcessObjectKey) {
    switch (postProcessingConfig.postProcessing) {
      case DELETE:
        postProcessDelete(postProcessObjectKey);
        break;
      case ARCHIVE:
        postProcessArchive(postProcessObjectKey);
        break;
      default:
        throw new IllegalStateException("Invalid post processing option : " +
          postProcessingConfig.postProcessing.name());
    }
  }

  private void postProcessArchive(String postProcessObjectKey) {
    String destBucket = config.bucket;
    switch (postProcessingConfig.archivingOption) {
      case MOVE_TO_DIRECTORY:
        //no-op
        break;
      case MOVE_TO_BUCKET:
        destBucket = postProcessingConfig.postProcessBucket;
        break;
      default:
        throw new IllegalStateException("Invalid Archive option : " + postProcessingConfig.archivingOption.name());
    }
    String srcObjKey = postProcessObjectKey.substring(postProcessObjectKey.lastIndexOf(config.delimiter) + 1);
    String destKey = postProcessingConfig.postProcessFolder + srcObjKey;
    AmazonS3Util.move(s3Client, config.bucket, postProcessObjectKey, destBucket, destKey);
  }

  private void postProcessDelete(String postProcessObjectKey) {
    LOG.debug("Deleting previous file '{}'", postProcessObjectKey);
    s3Client.deleteObject(config.bucket, postProcessObjectKey);
  }

  public void handleCurrentObjectAsError() {
    String srcObjKey = currentObject.getKey().substring(currentObject.getKey().lastIndexOf(config.delimiter) + 1);
    String destKey = errorConfig.errorFolder + srcObjKey;
    AmazonS3Util.move(s3Client, config.bucket, currentObject.getKey(), errorConfig.errorBucket, destKey);
    currentObject = null;
  }

  public void postProcessOlderObjectIfNeeded(AmazonS3Source.S3Offset s3Offset) {
    //If sdc was shutdown after reading an object but before post processing it, handle it now.

    //The scenario is detected as follows:
    //  1. the current key must not be null
    //  2. offset must be -1
    //  3. An object with same key must exist in s3
    //  4. The timestamp of the object ins3 must be same as that of the timestamp in offset [It is possible that one
    //    uploads another object with the same name. We can avoid post processing it without producing records by
    //    comparing the timestamp on that object

    if(s3Offset.getKey() != null &&
      "-1".equals(s3Offset.getOffset())) {
      //conditions 1, 2 are met. Check for 3 and 4.
      S3ObjectSummary objectSummary = AmazonS3Util.getObjectSummary(s3Client, config.bucket, s3Offset.getKey());
      if(objectSummary != null &&
        objectSummary.getLastModified().compareTo(new Date(Long.parseLong(s3Offset.getTimestamp()))) == 0) {
        postProcess(s3Offset.getKey());
      }
    }
    currentObject = null;
  }
}
