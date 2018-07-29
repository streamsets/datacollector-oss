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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.io.fileref.FileRefStreamCloseEventHandler;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.stage.cloudstorage.lib.Errors;
import com.streamsets.pipeline.stage.cloudstorage.lib.GCSEvents;
import com.streamsets.pipeline.stage.cloudstorage.lib.GcsUtil;
import com.streamsets.pipeline.stage.lib.GoogleCloudCredentialsConfig;
import com.streamsets.pipeline.stage.pubsub.lib.Groups;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

public class GoogleCloudStorageTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorageTarget.class);
  private static final String PARTITION_TEMPLATE = "partitionTemplate";
  private static final String TIME_DRIVER = "timeDriverTemplate";

  private final GCSTargetConfig gcsTargetConfig;

  private Storage storage;

  private ELVars elVars;
  private ELEval partitionEval;
  private ELEval fileNameEval;
  private ELEval timeDriverElEval;
  private CredentialsProvider credentialsProvider;
  private Calendar calendar;

  public GoogleCloudStorageTarget(GCSTargetConfig gcsTargetConfig) {
    this.gcsTargetConfig = gcsTargetConfig;
  }

  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = gcsTargetConfig.init(getContext(), super.init());
    gcsTargetConfig.credentials.getCredentialsProvider(getContext(), issues).ifPresent(p -> credentialsProvider = p);

    elVars = getContext().createELVars();
    timeDriverElEval = getContext().createELEval(TIME_DRIVER);
    partitionEval = getContext().createELEval(PARTITION_TEMPLATE);
    calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of(gcsTargetConfig.timeZoneID)));

    try {
      storage = StorageOptions.newBuilder().setCredentials(credentialsProvider.getCredentials()).build().getService();
    } catch (IOException e) {
      issues.add(getContext().createConfigIssue(
          Groups.CREDENTIALS.name(),
          GoogleCloudCredentialsConfig.CONF_CREDENTIALS_CREDENTIALS_PROVIDER,
          Errors.GCS_01,
          e
      ));
    }

    if (gcsTargetConfig.dataFormat == DataFormat.WHOLE_FILE) {
      fileNameEval = getContext().createELEval("fileNameEL");
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
    String pathExpression = GcsUtil.normalizePrefix(gcsTargetConfig.commonPrefix)
        + gcsTargetConfig.partitionTemplate;
    if (gcsTargetConfig.dataFormat == DataFormat.WHOLE_FILE) {
      handleWholeFileFormat(batch, elVars);
    } else {
      Multimap<String, Record> pathToRecordMap =
          ELUtils.partitionBatchByExpression(
              partitionEval,
              elVars,
              pathExpression,
              timeDriverElEval,
              elVars,
              gcsTargetConfig.timeDriverTemplate,
              Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of(gcsTargetConfig.timeZoneID))),
              batch
          );

      pathToRecordMap.keySet().forEach(path -> {
        Collection<Record> records = pathToRecordMap.get(path);
        String fileName = GcsUtil.normalizePrefix(path) + gcsTargetConfig.fileNamePrefix + '_' + UUID.randomUUID();
        if (StringUtils.isNotEmpty(gcsTargetConfig.fileNameSuffix)) {
          fileName = fileName + "." + gcsTargetConfig.fileNameSuffix;
        }
        try {
          ByteArrayOutputStream bOut = new ByteArrayOutputStream();
          OutputStream os = bOut;
          if (gcsTargetConfig.compress) {
            fileName = fileName + ".gz";
            os = new GZIPOutputStream(bOut);
          }
          BlobId blobId = BlobId.of(gcsTargetConfig.bucketTemplate, fileName);
          BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(getContentType()).build();
          final AtomicInteger recordsWithoutErrors = new AtomicInteger(0);
          try (DataGenerator dg = gcsTargetConfig.dataGeneratorFormatConfig
              .getDataGeneratorFactory().getGenerator(os)) {
            records.forEach(record -> {
              try {
                dg.write(record);
                recordsWithoutErrors.incrementAndGet();
              } catch (DataGeneratorException | IOException e) {
                LOG.error("Error writing record {}. Reason {}", record.getHeader().getSourceId(), e);
                getContext().toError(record, Errors.GCS_02, record.getHeader().getSourceId(), e);
              }
            });
          } catch (IOException e) {
            LOG.error("Error happened when creating Output stream. Reason {}", e);
            records.forEach(record -> getContext().toError(record, e));
          }

          try {
            if (recordsWithoutErrors.get() > 0) {
              Blob blob = storage.create(blobInfo, bOut.toByteArray());
              GCSEvents.GCS_OBJECT_WRITTEN.create(getContext())
                  .with(GCSEvents.BUCKET, blob.getBucket())
                  .with(GCSEvents.OBJECT_KEY, blob.getName())
                  .with(GCSEvents.RECORD_COUNT, recordsWithoutErrors.longValue())
                  .createAndSend();
            }
          } catch (StorageException e) {
            LOG.error("Error happened when writing to Output stream. Reason {}", e);
            records.forEach(record -> getContext().toError(record, e));
          }
        } catch (IOException e) {
          LOG.error("Error happened when creating Output stream. Reason {}", e);
          records.forEach(record -> getContext().toError(record, e));
        }
      });
    }
  }

  private void handleWholeFileFormat(Batch batch, ELVars elVars) {
    batch.getRecords().forEachRemaining(record -> {
      RecordEL.setRecordInContext(elVars, record);
      TimeEL.setCalendarInContext(elVars, calendar);
      try {
        Date recordDate = ELUtils.getRecordTime(timeDriverElEval, elVars, gcsTargetConfig.timeDriverTemplate, record);
        TimeNowEL.setTimeNowInContext(elVars, recordDate);
        String path = partitionEval.eval(elVars, gcsTargetConfig.partitionTemplate, String.class);
        String fileName = fileNameEval.eval(elVars, gcsTargetConfig.dataGeneratorFormatConfig.fileNameEL, String.class);
        String filePath = GcsUtil.normalizePrefix(GcsUtil.normalizePrefix(gcsTargetConfig.commonPrefix) + path) + fileName;
        BlobId blobId = BlobId.of(gcsTargetConfig.bucketTemplate, filePath);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        Blob blob = storage.get(blobId);

        if (blob != null &&
            gcsTargetConfig.dataGeneratorFormatConfig.wholeFileExistsAction
                == WholeFileExistsAction.TO_ERROR) {
          //File already exists and error action is to Error
          getContext().toError(record, Errors.GCS_03, filePath);
          return;
        } //else overwrite

        EventRecord eventRecord = GCSEvents.FILE_TRANSFER_COMPLETE_EVENT.create(getContext())
            .with(FileRefUtil.WHOLE_FILE_SOURCE_FILE_INFO, record.get(FileRefUtil.FILE_INFO_FIELD_PATH).getValueAsMap())
            .withStringMap(
                FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO,
                ImmutableMap.of(
                    GCSEvents.BUCKET, blobId.getBucket(),
                    GCSEvents.OBJECT_KEY, blobId.getName()
                )
            ).create();

        FileRefStreamCloseEventHandler fileRefStreamCloseEventHandler =
            new FileRefStreamCloseEventHandler(eventRecord);

        boolean errorHappened = false;

        try (DataGenerator dg =
                 gcsTargetConfig.dataGeneratorFormatConfig.getDataGeneratorFactory().getGenerator(
                     Channels.newOutputStream(storage.writer(blobInfo)),
                     //Close handler for populating checksum info in the event.
                     fileRefStreamCloseEventHandler
                 )
        ) {
          dg.write(record);
        } catch (IOException | DataGeneratorException e) {
          LOG.error("Error happened when Writing to Output stream. Reason {}", e);
          getContext().toError(record, Errors.GCS_02, e);
          errorHappened = true;
        }
        if (!errorHappened){
          //Put the event if the record is not sent to error.
          getContext().toEvent(eventRecord);
        }
      } catch (ELEvalException e) {
        LOG.error("Error happened when evaluating Expressions. Reason {}", e);
        getContext().toError(record, Errors.GCS_04, e);
      } catch (OnRecordErrorException e) {
        LOG.error("Error happened when evaluating Expressions. Reason {}", e);
        getContext().toError(e.getRecord(), e.getErrorCode(), e.getParams());
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
