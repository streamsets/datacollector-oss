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
package com.streamsets.pipeline.stage.destination.s3;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.WholeFileExistsAction;
import com.streamsets.pipeline.config.ChecksumAlgorithm;
import com.streamsets.pipeline.lib.event.WholeFileProcessedEvent;
import com.streamsets.pipeline.lib.io.fileref.FileRefStreamCloseEventHandler;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

final class WholeFileHelper extends FileHelper {
  private final DataFormatGeneratorService generatorService;
  private static final String SIZE = "size";
  private static final Logger LOGGER = LoggerFactory.getLogger(WholeFileHelper.class);

  WholeFileHelper(
      Target.Context context,
      S3TargetConfigBean s3TargetConfigBean,
      TransferManager transferManager,
      List<Stage.ConfigIssue> configIssues
  ) {
    super(context, s3TargetConfigBean, transferManager);
    generatorService = context.getService(DataFormatGeneratorService.class);
    //init adds the config issues
    init(configIssues);
  }

  public void init(List<Stage.ConfigIssue> issues) {
    if (s3TargetConfigBean.compress) {
      issues.add(
          context.createConfigIssue(
              Groups.S3.getLabel(),
              S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "compress",
              Errors.S3_50
          )
      );
    }
  }

  private String getFileNameFromFileNameEL(String keyPrefix, Record record) throws StageException {
    return keyPrefix + generatorService.wholeFileFilename(record);
  }

  private void checkForWholeFileExistence(String bucket, String objectKey) throws OnRecordErrorException {
    boolean fileExists = s3TargetConfigBean.s3Config.getS3Client().doesObjectExist(bucket, objectKey);
    LOGGER.debug("Validating object existence for '{}' = {}", objectKey, fileExists);
    WholeFileExistsAction wholeFileExistsAction = generatorService.wholeFileExistsAction();
    if (fileExists && wholeFileExistsAction == WholeFileExistsAction.TO_ERROR) {
      throw new OnRecordErrorException(Errors.S3_51, objectKey);
    }
    //else we will do the upload which will overwrite/ version based on the bucket configuration.
  }

  private EventRecord createEventRecordForFileTransfer(Record record, String bucket, String objectKey) {
    return WholeFileProcessedEvent.FILE_TRANSFER_COMPLETE_EVENT
        .create(context)
        .with(WholeFileProcessedEvent.SOURCE_FILE_INFO, record.get(FileRefUtil.FILE_INFO_FIELD_PATH).getValueAsMap())
        .withStringMap(WholeFileProcessedEvent.TARGET_FILE_INFO, ImmutableMap.of(BUCKET, bucket, OBJECT_KEY, objectKey))
        .create();
  }

  @Override
  public List<UploadMetadata> handle(
      Iterator<Record> recordIterator,
      String bucket,
      String keyPrefix
  ) throws IOException, StageException {
    List<UploadMetadata> uploads = new ArrayList<>();
    //Only one record per batch if whole file
    if (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      try {

        try {
          FileRefUtil.validateWholeFileRecord(record);
        } catch (IllegalArgumentException e) {
          LOGGER.error("Validation Failed For Record {}", e);
          throw new OnRecordErrorException(record, Errors.S3_52, e);
        }

        String fileName = getFileNameFromFileNameEL(keyPrefix, record);

        checkForWholeFileExistence(bucket, fileName);

        FileRef fileRef = record.get(FileRefUtil.FILE_REF_FIELD_PATH).getValueAsFileRef();

        ObjectMetadata metadata = getObjectMetadata();
        metadata = (metadata == null) ? new ObjectMetadata() : metadata;

        //Mandatory field path specifying size.
        metadata.setContentLength(record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + SIZE).getValueAsLong());

        EventRecord eventRecord = createEventRecordForFileTransfer(record, bucket, fileName);

        //Fyi this gets closed automatically after upload completes.
        InputStream is = FileRefUtil.getReadableStream(
            context,
            fileRef,
            InputStream.class,
            generatorService.wholeFileIncludeChecksumInTheEvents(),
            ChecksumAlgorithm.forApi(generatorService.wholeFileChecksumAlgorithm()),
            new FileRefStreamCloseEventHandler(eventRecord)
        );
        //We are bypassing the generator because S3 has a convenient notion of taking input stream as a parameter.
        Upload upload = doUpload(bucket, fileName, is, metadata);
        uploads.add(new UploadMetadata(
          upload,
          bucket,
          ImmutableList.of(record),
          ImmutableList.of(eventRecord)
        ));

        //Add event to event lane.
      } catch (OnRecordErrorException e) {
        LOGGER.error("Error on record: {}", e);
        errorRecordHandler.onError(
            new OnRecordErrorException(
                record,
                e.getErrorCode(),
                e.getParams()
            )
        );
      }
    }
    return uploads;
  }
}
