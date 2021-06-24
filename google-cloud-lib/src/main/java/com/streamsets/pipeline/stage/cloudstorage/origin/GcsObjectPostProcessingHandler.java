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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.streamsets.pipeline.stage.cloudstorage.lib.GcsOps;
import com.streamsets.pipeline.stage.cloudstorage.lib.GcsUtil;

/**
 * Helper class for handling post processing of files (specfically errror files)
 */
public final class GcsObjectPostProcessingHandler {

  private final GcsOps gcsOps;
  private final GcsOriginPostProcessingConfig gcsOriginPostProcessingConfig;

  GcsObjectPostProcessingHandler(Storage storage, GcsOriginPostProcessingConfig gcsOriginPostProcessingConfig) {
    this.gcsOps = new GcsOps(storage);
    this.gcsOriginPostProcessingConfig = gcsOriginPostProcessingConfig;
  }

  private String getDestinationPath(BlobId sourceId, String destPrefix) {
    String fileName = sourceId.getName().substring(sourceId.getName().lastIndexOf(GcsUtil.PATH_DELIMITER) + 1);
    return GcsUtil.normalizePrefix(destPrefix) + fileName;
  }
  
  /**
   * Handle Blob
   * @param blobId blob id
   */
  void handle(BlobId blobId) {
    switch (gcsOriginPostProcessingConfig.postProcessing) {
      case NONE:
        break;
      case ARCHIVE:
        handleArchive(blobId);
        break;
      case DELETE:
        gcsOps.delete(blobId);
        break;
    }
  }
  /**
   * Archive the blob
   * @param blobId blobId
   */
  private void handleArchive(BlobId blobId) {
    String destinationPath = getDestinationPath(blobId, gcsOriginPostProcessingConfig.postProcessPrefix);
    switch (gcsOriginPostProcessingConfig.archivingOption) {
      case COPY_TO_BUCKET:
        gcsOps.copy(blobId, gcsOriginPostProcessingConfig.postProcessBucket, destinationPath, false);
        break;
      case MOVE_TO_BUCKET:
        gcsOps.copy(blobId, gcsOriginPostProcessingConfig.postProcessBucket, destinationPath, true);
        break;
      case COPY_TO_PREFIX:
        gcsOps.copy(blobId, blobId.getBucket(), destinationPath, false);
        break;
      case MOVE_TO_PREFIX:
        gcsOps.copy(blobId, blobId.getBucket(), destinationPath, true);
        break;
    }
  }

}
