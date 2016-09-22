/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.io.fileref.FileRefStreamCloseEventHandler;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

final class WholeFileHelper extends FileHelper {
  private ELEval fileNameELEval;
  private static final String SIZE = "size";

  WholeFileHelper(Stage.Context context, S3TargetConfigBean s3TargetConfigBean, TransferManager transferManager, List<Stage.ConfigIssue> configIssues) {
    super(context, s3TargetConfigBean, transferManager);
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
    //This initializes the metrics for the stage when the data format is whole file.
    DataGeneratorFactory dgFactory = s3TargetConfigBean.dataGeneratorFormatConfig.getDataGeneratorFactory();
    try (DataGenerator dg = dgFactory.getGenerator(new ByteArrayOutputStream())) {
      //NOOP
    } catch (IOException e) {
      issues.add(
          context.createConfigIssue(
              Groups.S3.getLabel(),
              S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "dataFormat",
              Errors.S3_40,
              e.getMessage()
          )
      );
    }
    this.fileNameELEval = context.createELEval("fileNameEL");
  }

  private String getFileNameFromFileNameEL(String keyPrefix, Record record) throws StageException {
    Utils.checkState(fileNameELEval != null, "File Name EL Evaluator is null");
    ELVars vars = context.createELVars();
    RecordEL.setRecordInContext(vars, record);
    StringBuilder fileName = new StringBuilder();
    fileName = fileName.append(keyPrefix);
    fileName.append(fileNameELEval.eval(vars, s3TargetConfigBean.dataGeneratorFormatConfig.fileNameEL, String.class));
    return fileName.toString();
  }

  private void checkForWholeFileExistence(String objectKey) throws OnRecordErrorException {
    boolean fileExists = s3TargetConfigBean.s3Config.getS3Client().doesObjectExist(s3TargetConfigBean.s3Config.bucket, objectKey);
    WholeFileExistsAction wholeFileExistsAction =s3TargetConfigBean.dataGeneratorFormatConfig.wholeFileExistsAction;
    if (fileExists && wholeFileExistsAction == WholeFileExistsAction.TO_ERROR) {
      throw new OnRecordErrorException(Errors.S3_51, objectKey);
    }
    //else we will do the upload which will overwrite/ version based on the bucket configuration.
  }

  private EventRecord createEventRecordForFileTransfer(Record record, String objectKey) {
    return S3Events.FILE_TRANSFER_COMPLETE_EVENT
        .create(context)
        .with(FileRefUtil.WHOLE_FILE_SOURCE_FILE_INFO, record.get(FileRefUtil.FILE_INFO_FIELD_PATH).getValueAsMap())
        .withStringMap(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO, ImmutableMap.of(BUCKET, (Object)s3TargetConfigBean.s3Config.bucket, OBJECT_KEY, objectKey))
        .create();
  }

  @Override
  public List<Upload> handle(
      Iterator<Record> recordIterator,
      String keyPrefix
  ) throws IOException, StageException {
    List<Upload> uploads = new ArrayList<Upload>();
    //Only one record per batch if whole file
    if (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      String fileName = getFileNameFromFileNameEL(keyPrefix, record);
      try {
        checkForWholeFileExistence(fileName);
        FileRef fileRef = record.get(FileRefUtil.FILE_REF_FIELD_PATH).getValueAsFileRef();

        ObjectMetadata metadata = getObjectMetadata();
        metadata = (metadata == null) ? new ObjectMetadata() : metadata;
        //Mandatory field path specifying size.
        metadata.setContentLength(record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + SIZE).getValueAsLong());

        EventRecord eventRecord = createEventRecordForFileTransfer(record, fileName);

        InputStream is = FileRefUtil.getReadableStream(
            context,
            fileRef,
            InputStream.class,
            s3TargetConfigBean.dataGeneratorFormatConfig.includeChecksumInTheEvents,
            s3TargetConfigBean.dataGeneratorFormatConfig.checksumAlgorithm,
            new FileRefStreamCloseEventHandler(eventRecord)
        );
        //We are bypassing the generator because S3 has a convenient notion of taking input stream as a parameter.
        Upload upload = doUpload(fileName, is, metadata);
        uploads.add(upload);

        //Add event to event lane.
        context.toEvent(eventRecord);
      } catch (OnRecordErrorException e) {
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