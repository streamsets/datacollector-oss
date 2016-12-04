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
import com.streamsets.pipeline.api.el.ELEvalException;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

final class DefaultOutputStreamHandler implements OutputStreamHelper {
  private ADLStoreClient client;
  private String uniquePrefix;
  private Map<String, String> filePathMap = new HashMap<>();

  public DefaultOutputStreamHandler(ADLStoreClient client, String uniquePrefix) {
    this.client = client;
    this.uniquePrefix = uniquePrefix;
  }

  @Override
  public ADLFileOutputStream getStream(String filePath)
      throws StageException, IOException {
    ADLFileOutputStream stream;
    if (!client.checkExists(filePath)) {
      stream = client.createFile(filePath, IfExists.FAIL);
    } else {
      stream = client.getAppendStream(filePath);
    }
    return stream;
  }

  @Override
  public String getFilePath(String dirPath, Record record, Date recordTime) throws ELEvalException {
    String filePath = filePathMap.get(dirPath);
    if (filePath == null) {
      filePath = uniquePrefix + "-" + UUID.randomUUID();
      filePathMap.put(dirPath, filePath);
    }
    return dirPath + "/" + filePath;
  }

  @Override
  public void clearStatus() {
    filePathMap.clear();
  }
}
