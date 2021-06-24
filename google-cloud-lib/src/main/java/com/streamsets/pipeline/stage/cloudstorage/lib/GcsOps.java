/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.pipeline.stage.cloudstorage.lib;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.streamsets.pipeline.stage.cloudstorage.origin.GcsObjectPostProcessingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsOps {

    private static final Logger LOG = LoggerFactory.getLogger(GcsObjectPostProcessingHandler.class);
    private static final String BLOB_PATH_TEMPLATE = "gs://%s/%s";

    private final Storage storage;

    public GcsOps(Storage storage) {
        this.storage = storage;
    }

    public void copy(BlobId sourceBlobId, String destinationBucket, String destinationPath, boolean deleteSource) {
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

    /**
     * Delete a blob for gcs
     *
     * @param blobId blobId
     */
    public void delete(BlobId blobId) {
        LOG.debug("Deleting object '{}'", String.format(BLOB_PATH_TEMPLATE, blobId.getBucket(), blobId.getName()));
        boolean deleted = storage.delete(blobId);
        if (!deleted) {
            LOG.error("Cannot delete object '{}'", String.format(BLOB_PATH_TEMPLATE, blobId.getBucket(), blobId.getName()));
        }
    }

    public Blob createObject(String bucket, String objectPath, byte[] content) {
        BlobId blobId = BlobId.of(bucket, objectPath);
        return storage.create(BlobInfo.newBuilder(blobId).build(), content);
    }

    public Blob getBlob(String bucket, String objectPath) {
        return storage.get(BlobId.of(bucket, objectPath));
    }

    public Blob update(BlobInfo blobInfo) {
        return storage.update(blobInfo);
    }

}
