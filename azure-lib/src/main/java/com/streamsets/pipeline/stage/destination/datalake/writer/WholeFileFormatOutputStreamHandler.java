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

  private ELEval dirPathTemplateEval;
  private ELVars dirPathTemplateVars;

  public WholeFileFormatOutputStreamHandler(ADLStoreClient client, String uniquePrefix, String fileNameEL, ELEval dirPathTemplateEval, ELVars dirPathTemplateVars, WholeFileExistsAction wholeFileAlreadyExistsAction) {
    this.client = client;
    this.uniquePrefix = uniquePrefix;
    this.fileNameEL = fileNameEL;
    this.dirPathTemplateEval = dirPathTemplateEval;
    this.dirPathTemplateVars = dirPathTemplateVars;
    this.wholeFileAlreadyExistsAction = wholeFileAlreadyExistsAction;
  }

  @Override
  public ADLFileOutputStream getStream(String filePath)
      throws StageException, IOException {
    if (client.checkExists(filePath)) {
      if (wholeFileAlreadyExistsAction == WholeFileExistsAction.OVERWRITE) {
        client.delete(filePath);
        stream = client.createFile(filePath, IfExists.OVERWRITE);
        LOG.debug(Utils.format(Errors.ADLS_05.getMessage(), filePath) + "so deleting it");
      } else {
        throw new OnRecordErrorException(Errors.ADLS_05, filePath);
      }
    } else {
      stream = client.createFile(filePath, IfExists.FAIL);
    }
    return stream;
  }

  @Override
  public String getFilePath(String dirPath, Record record, Date recordTime) throws ELEvalException {
    RecordEL.setRecordInContext(dirPathTemplateVars, record);
    TimeNowEL.setTimeNowInContext(dirPathTemplateVars, recordTime);
    String fileName = dirPathTemplateEval.eval(dirPathTemplateVars, fileNameEL, String.class);
    String filePath = uniquePrefix + "-" + fileName;
    return dirPath + "/" + filePath;
  }

  @Override
  public void clearStatus() {
    //no-op
  }
}
