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

package com.streamsets.pipeline.stage.cloudstorage.destination;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.storage.*;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.cloudstorage.lib.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

public class GoogleCloudStorageTarget extends BaseTarget {
    private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorageTarget.class);
    private static final String PARTITION_TEMPLATE = "partitionTemplate";


    private final GCSTargetConfig gcsTargetConfig;

    private Storage storage;

    private ELVars elVars;
    private ELEval partitionEval;
    private Calendar calendar;
    private CredentialsProvider credentialsProvider;

    public GoogleCloudStorageTarget(GCSTargetConfig gcsTargetConfig) {
        this.gcsTargetConfig = gcsTargetConfig;
    }

    /** {@inheritDoc} */
    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = gcsTargetConfig.init(getContext(), super.init());
        gcsTargetConfig.credentials.getCredentialsProvider(getContext(), issues).ifPresent(p -> credentialsProvider = p);

        storage = StorageOptions.getDefaultInstance().getService();

        elVars = getContext().createELVars();
        partitionEval = getContext().createELEval(PARTITION_TEMPLATE);

        calendar = Calendar.getInstance(TimeZone.getTimeZone(gcsTargetConfig.timeZoneID));

        try {
            storage = StorageOptions.newBuilder().setCredentials(credentialsProvider.getCredentials()).build().getService();
        } catch (IOException e) {
            getContext().reportError(Errors.GCS_01, e);
        }


        return issues;
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        // Clean up any open resources.
        super.destroy();
    }

    /** {@inheritDoc} */
    @Override
    public void write(Batch batch) throws StageException {
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();

        Iterator<Record> batchIterator = batch.getRecords();
        while (batchIterator.hasNext()) {
            Record record = batchIterator.next();

            DataGeneratorFactory generatorFactory = gcsTargetConfig.dataGeneratorFormatConfig.getDataGeneratorFactory();
            try (DataGenerator gen = generatorFactory.getGenerator(bOut)) {
                gen.write(record);
            } catch (IOException e) {
                getContext().toError(record, Errors.GCS_00, e);
            }
        }

        if (bOut.size() > 0) {
            TimeEL.setCalendarInContext(elVars, calendar);
            TimeNowEL.setTimeNowInContext(elVars, calendar.getTime());

            String partition = partitionEval.eval(elVars, gcsTargetConfig.partitionTemplate, String.class);
            String path = gcsTargetConfig.commonPrefix + partition + gcsTargetConfig.fileNamePrefix + UUID.randomUUID();

            BlobId blobId = BlobId.of(gcsTargetConfig.bucketTemplate, path);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(getContentType()).build();
            try {
                Blob blob = storage.create(blobInfo, bOut.toByteArray());
            } catch(StorageException e) {
                batchIterator = batch.getRecords();
                while (batchIterator.hasNext()) {
                    getContext().toError(batchIterator.next(), e);
                }
            }
        }
    }

    private String getContentType() {
        switch (gcsTargetConfig.dataFormat) {
            case JSON:
                return "text/json";
            default:
                break;
        }

        return null;
    }
}
