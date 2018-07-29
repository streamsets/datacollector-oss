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
package com.streamsets.pipeline.stage.destination.datalake.writer;

import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;
import com.streamsets.pipeline.lib.io.fileref.FileRefStreamCloseEventHandler;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.stage.destination.datalake.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

final class WholeFileFormatOutputStreamHandler implements OutputStreamHelper {
  private static final Logger LOG = LoggerFactory.getLogger(WholeFileFormatOutputStreamHandler.class);

  private Target.Context context;
  private ADLStoreClient client;
  private ADLFileOutputStream stream;
  private String uniquePrefix;
  private String fileNameEL;
  private WholeFileExistsAction wholeFileAlreadyExistsAction;
  private String tmpFileName;
  private String tmpFilePath;

  private ELEval fileNameEval;
  private ELVars fileNameVars;

  private EventRecord wholeFileEventRecord;

  public WholeFileFormatOutputStreamHandler(
      Target.Context context,
      ADLStoreClient client,
      String uniquePrefix,
      String fileNameEL,
      ELEval fileNameEval,
      ELVars fileNameVars,
      WholeFileExistsAction wholeFileAlreadyExistsAction
  ) {
    this.context = context;
    this.client = client;
    this.uniquePrefix = uniquePrefix;
    this.fileNameEL = fileNameEL;
    this.fileNameEval = fileNameEval;
    this.fileNameVars = fileNameVars;
    this.wholeFileAlreadyExistsAction = wholeFileAlreadyExistsAction;
  }

  @Override
  public ADLFileOutputStream getOutputStream(String tempFilePath) throws StageException, IOException {
    String filePath = tmpFilePath.replaceFirst(TMP_FILE_PREFIX, "");
    if (client.checkExists(filePath)) {
      if (wholeFileAlreadyExistsAction == WholeFileExistsAction.OVERWRITE) {
        client.delete(filePath);
        LOG.debug(Utils.format(Errors.ADLS_05.getMessage(), filePath) + "so deleting it");
      } else {
        throw new OnRecordErrorException(Errors.ADLS_05, filePath);
      }
    }
    stream = client.createFile(tmpFilePath, IfExists.OVERWRITE);
    return stream;
  }

  @Override
  public void commitFile(String dirPath) throws IOException {
    if (dirPath != null && tmpFileName != null) {
      boolean overwrite = wholeFileAlreadyExistsAction == WholeFileExistsAction.OVERWRITE;
      String filePath = dirPath + "/" + tmpFileName.replaceFirst(TMP_FILE_PREFIX, "");
      client.rename(dirPath + "/" + tmpFileName, filePath, overwrite);

      //Throw file copied event here.
      context.toEvent(wholeFileEventRecord);
    }
  }

  @Override
  public String getTempFilePath(String dirPath, Record record, Date recordTime) throws StageException {
    RecordEL.setRecordInContext(fileNameVars, record);
    TimeNowEL.setTimeNowInContext(fileNameVars, recordTime);
    String fileName = fileNameEval.eval(fileNameVars, fileNameEL, String.class);
    tmpFileName = TMP_FILE_PREFIX + uniquePrefix + "-" + fileName;
    tmpFilePath = dirPath + "/" + tmpFileName;

    wholeFileEventRecord = createWholeFileEventRecord(record, tmpFilePath.replaceFirst(TMP_FILE_PREFIX, ""));

    return tmpFilePath;
  }

  @Override
  public void clearStatus() throws IOException {
    tmpFilePath = null;
  }

  @Override
  public boolean shouldRoll(String dirPath) {
    return true;
  }

  @Override
  public StreamCloseEventHandler<?> getStreamCloseEventHandler() {
    return new FileRefStreamCloseEventHandler(wholeFileEventRecord);
  }

  private EventRecord createWholeFileEventRecord(Record record, String renamableFinalPath) throws StageException {
    try {
      FileRefUtil.validateWholeFileRecord(record);
    } catch (IllegalArgumentException e) {
      throw new OnRecordErrorException(record, Errors.ADLS_00, e);
    }
    //Update the event record with source file info information
    return DataLakeEvents.FILE_TRANSFER_COMPLETE_EVENT
        .create(context)
        .with(FileRefUtil.WHOLE_FILE_SOURCE_FILE_INFO, record.get(FileRefUtil.FILE_INFO_FIELD_PATH).getValueAsMap())
        .withStringMap(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO, ImmutableMap.of("path", renamableFinalPath))
        .create();
  }
}
