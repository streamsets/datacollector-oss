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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.streamsets.pipeline.stage.cloudstorage.lib.GcsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for handling post processing of files (specfically errror files)
 */
public final class GcsObjectPostProcessingHandler {
  private static final Logger LOG = LoggerFactory.getLogger(GcsObjectPostProcessingHandler.class);
  private static final String BLOB_PATH_TEMPLATE = "gs://%s/%s";

  private final Storage storage;
  private final GcsOriginErrorConfig gcsOriginErrorConfig;

  GcsObjectPostProcessingHandler(Storage storage, GcsOriginErrorConfig gcsOriginErrorConfig) {
    this.storage = storage;
    this.gcsOriginErrorConfig = gcsOriginErrorConfig;
  }

  /**
   * Delete a blob for gcs
   * @param blobId blobId
   */
  private void delete(BlobId blobId) {
    LOG.debug("Deleting object '{}'", String.format(BLOB_PATH_TEMPLATE, blobId.getBucket(), blobId.getName()));
    boolean deleted = storage.delete(blobId);
    if (!deleted) {
      LOG.error("Cannot delete object '{}'", String.format(BLOB_PATH_TEMPLATE, blobId.getBucket(), blobId.getName()));
    }
  }

  /**
   * Copy a blob for gcs to a destination bucket and a path (and delete the source blob if needed)
   * @param sourceBlobId sourceBlobId
   * @param destinationBucket destination bucket
   * @param destinationPath destination path
   * @param deleteSource delete source blob
   */
  private void copy(BlobId sourceBlobId, String destinationBucket, String destinationPath, boolean deleteSource) {
    LOG.debug(
        "Copying object '{}' to Object '{}'",
        String.format(BLOB_PATH_TEMPLATE, sourceBlobId.getBucket(), sourceBlobId.getName()),
        String.format(BLOB_PATH_TEMPLATE, destinationBucket, destinationPath)
    );

    Storage.CopyRequest copyRequest = new Storage.CopyRequest.Builder()
        .setSource(sourceBlobId)
        .setTarget(BlobId.of(destinationBucket, destinationPath))
        .build();
    Blob destinationBlob = storage.copy(copyRequest).getResult();
    LOG.debug(
        "Copied object '{}' to Object '{}'",
        String.format(BLOB_PATH_TEMPLATE, sourceBlobId.getBucket(), sourceBlobId.getName()),
        String.format(BLOB_PATH_TEMPLATE, destinationBlob.getBlobId().getBucket(), destinationBlob.getBlobId().getName())
    );
    if (deleteSource) {
      delete(sourceBlobId);
    }
  }

  private String getDestinationPath(BlobId sourceId, String destPrefix) {
    String fileName = sourceId.getName().substring(sourceId.getName().lastIndexOf(GcsUtil.PATH_DELIMITER) + 1, sourceId.getName().length());
    return GcsUtil.normalizePrefix(destPrefix) + fileName;
  }
  
  /**
   * Handle error Blob
   * @param blobId blob id
   */
  void handleError(BlobId blobId) {
    switch (gcsOriginErrorConfig.errorHandlingOption) {
      case NONE:
        break;
      case ARCHIVE:
        handleArchive(blobId);
        break;
      case DELETE:
        delete(blobId);
        break;
    }
  }
  /**
   * Archive the blob
   * @param blobId blobId
   */
  private void handleArchive(BlobId blobId) {
    String destinationPath = getDestinationPath(blobId, gcsOriginErrorConfig.errorPrefix);
    switch (gcsOriginErrorConfig.archivingOption) {
      case COPY_TO_BUCKET:
        copy(blobId, gcsOriginErrorConfig.errorBucket, destinationPath, false);
        break;
      case MOVE_TO_BUCKET:
        copy(blobId, gcsOriginErrorConfig.errorBucket, destinationPath, true);
        break;
      case COPY_TO_PREFIX:
        copy(blobId, blobId.getBucket(), destinationPath, false);
        break;
      case MOVE_TO_PREFIX:
        copy(blobId, blobId.getBucket(), destinationPath, true);
        break;
    }
  }

}
