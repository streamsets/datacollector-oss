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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.stage.destination.datalake.writer;

import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.stage.destination.datalake.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

final class WholeFileFormatOutputStreamHandler implements OutputStreamHelper {
  private static final Logger LOG = LoggerFactory.getLogger(WholeFileFormatOutputStreamHandler.class);

  private ADLStoreClient client;
  private ADLFileOutputStream stream;
  private String uniquePrefix;
  private String fileNameEL;
  private WholeFileExistsAction wholeFileAlreadyExistsAction;
  private String tmpFileName;
  private String tmpFilePath;

  private ELEval fileNameEval;
  private ELVars fileNameVars;

  public WholeFileFormatOutputStreamHandler(
      ADLStoreClient client,
      String uniquePrefix,
      String fileNameEL,
      ELEval fileNameEval,
      ELVars fileNameVars,
      WholeFileExistsAction wholeFileAlreadyExistsAction
  ) {
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
    }
  }

  @Override
  public String getTempFilePath(String dirPath, Record record, Date recordTime) throws ELEvalException {
    RecordEL.setRecordInContext(fileNameVars, record);
    TimeNowEL.setTimeNowInContext(fileNameVars, recordTime);
    String fileName = fileNameEval.eval(fileNameVars, fileNameEL, String.class);
    tmpFileName = TMP_FILE_PREFIX + uniquePrefix + "-" + fileName;
    tmpFilePath = dirPath + "/" + tmpFileName;
    return tmpFilePath;
  }

  @Override
  public void clearStatus() throws IOException {
    tmpFilePath = null;
  }

  @Override
  public boolean shouldRoll(String dirPath) {
    return false;
  }
}
