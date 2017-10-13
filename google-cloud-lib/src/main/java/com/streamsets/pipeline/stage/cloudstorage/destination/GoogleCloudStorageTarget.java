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
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.stage.cloudstorage.lib.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

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

  @Override
  public void destroy() {
    // Clean up any open resources.
    super.destroy();
  }

  @Override
  public void write(Batch batch) throws StageException {
    TimeEL.setCalendarInContext(elVars, calendar);
    TimeNowEL.setTimeNowInContext(elVars, calendar.getTime());

    String pathExpression = gcsTargetConfig.commonPrefix + gcsTargetConfig.partitionTemplate;

    Multimap<String, Record> pathToRecordMap =
        ELUtils.partitionBatchByExpression(partitionEval, elVars, pathExpression, batch);

    pathToRecordMap.keySet().forEach(path -> {
      Collection<Record> records = pathToRecordMap.get(path);
      String fileName = path + gcsTargetConfig.fileNamePrefix + '_' + UUID.randomUUID();
      BlobId blobId = BlobId.of(gcsTargetConfig.bucketTemplate, fileName);
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(getContentType()).build();

      ByteArrayOutputStream bOut = new ByteArrayOutputStream();

      try (DataGenerator dg = gcsTargetConfig.dataGeneratorFormatConfig.getDataGeneratorFactory().getGenerator(bOut)) {
        records.forEach(record -> {
          try {
            dg.write(record);
          } catch (DataGeneratorException | IOException e) {
            LOG.error("Error writing record {}. Reason {}", record.getHeader().getSourceId(), e);
            getContext().toError(record, Errors.GCS_02, record.getHeader().getSourceId(), e);
          }
        });
      } catch (IOException e) {
        LOG.error("Error happened when creating Output stream. Reason {}", e);
      }

      try{
        if (bOut.size() > 0) {
          Blob blob = storage.create(blobInfo, bOut.toByteArray());
        }
        //TODO: events SDC-7559
      } catch (StorageException e) {
        LOG.error("Error happened when writing to Output stream. Reason {}", e);
        records.forEach(record -> getContext().toError(record, e));
      }
    });
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
