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
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;
import com.google.common.collect.MinMaxPriorityQueue;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.util.AntPathMatcher;
import com.streamsets.pipeline.stage.cloudstorage.lib.Errors;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;

public class GoogleCloudStorageSource extends BaseSource {

    private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorageSource.class);
    private Storage storage;
    private GCSOriginConfig gcsOriginConfig;
    private DataParser parser;
    private AntPathMatcher antPathMatcher;
    private MinMaxPriorityQueue<Blob> minMaxPriorityQueue;
    private CredentialsProvider credentialsProvider;

    public GoogleCloudStorageSource(GCSOriginConfig gcsOriginConfig) {
        this.gcsOriginConfig = gcsOriginConfig;
    }

    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = gcsOriginConfig.init(getContext(), super.init());
        minMaxPriorityQueue = MinMaxPriorityQueue.orderedBy((Blob o1, Blob o2) -> {
            int result = o1.getUpdateTime().compareTo(o2.getUpdateTime());
            if(result != 0) {
                //same modified time. Use name to sort
                return result;
            }
            return o1.getName().compareTo(o2.getName());
        }).maximumSize(gcsOriginConfig.maxResultQueueSize).create();
        antPathMatcher = new AntPathMatcher();
        gcsOriginConfig.prefixPattern = gcsOriginConfig.commonPrefix + gcsOriginConfig.prefixPattern;

        gcsOriginConfig.credentials.getCredentialsProvider(getContext(), issues).ifPresent(p -> credentialsProvider = p);

        try {
            storage = StorageOptions.newBuilder().setCredentials(credentialsProvider.getCredentials()).build().getService();
        } catch (IOException e) {
            getContext().reportError(Errors.GCS_01, e);
        }

        return issues;
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        maxBatchSize = gcsOriginConfig.basicConfig.maxBatchSize;

        long minTimestamp = 0;
        String blobId = "";
        String fileOffset = "0";
        Page<Blob> blobs;

        if (Strings.isNullOrEmpty(lastSourceOffset)) {
            // Get Min TimeStamp
            blobs = storage.list(gcsOriginConfig.bucketTemplate,
                    Storage.BlobListOption.prefix(gcsOriginConfig.commonPrefix));

            for (Blob blob : blobs.iterateAll()) {
                if (blob.getSize() > 0 && antPathMatcher.match(gcsOriginConfig.prefixPattern, blob.getName())) {
                    minMaxPriorityQueue.add(blob);
                }
            }

        } else {
            minTimestamp = getMinTimestampFromOffset(lastSourceOffset);
            fileOffset = getFileOffsetFromOffset(lastSourceOffset);
            blobId = getBlobIdFromOffset(lastSourceOffset);
        }

        if (minMaxPriorityQueue.size() == 0) {
            blobs = storage.list(gcsOriginConfig.bucketTemplate,
                    Storage.BlobListOption.prefix(gcsOriginConfig.commonPrefix));

            for (Blob blob : blobs.iterateAll()) {
                if (blob.getSize() > 0 && blob.getUpdateTime() >= minTimestamp && antPathMatcher.match(gcsOriginConfig.prefixPattern, blob.getName())) {
                    minMaxPriorityQueue.add(blob);
                }
            }
        }

        if (minMaxPriorityQueue.size() == 0) {
            // Nore more data avaliable
            return lastSourceOffset;
        }

        // Process Blob
        if (parser == null) {
            Blob blob = minMaxPriorityQueue.pollFirst();

            if (blobId.equals(blob.getGeneratedId()) && fileOffset.equals("-1")) {
                return lastSourceOffset;
            }

            fileOffset = "0";
            parser = gcsOriginConfig.dataParserFormatConfig.getParserFactory().getParser(blob.getGeneratedId(),
                    Channels.newInputStream(blob.reader()), fileOffset);

            blobId = blob.getGeneratedId();
            minTimestamp = blob.getUpdateTime();
        }

        try {
            int recordCount = 0;
            while (recordCount < maxBatchSize) {
                Record record = parser.parse();
                if (record != null) {
                    batchMaker.addRecord(record);
                    fileOffset = parser.getOffset();
                } else {
                    fileOffset = "-1";
                    parser.close();
                    parser = null;
                    break;
                }
                recordCount++;
            }
        } catch (IOException e) {
            getContext().reportError(Errors.GCS_00, e);
        }

        return String.format("%d::%s::%s", minTimestamp, fileOffset, blobId);
    }

    private long getMinTimestampFromOffset(String offset) {
        return Long.valueOf(offset.split("::", 3)[0]);
    }

    private String getFileOffsetFromOffset(String offset) {
        return offset.split("::", 3)[1];
    }

    private String getBlobIdFromOffset(String offset) {
        return offset.split("::", 3)[2];
    }

    @Override
    public void destroy() {
        IOUtils.closeQuietly(parser);
    }
}
