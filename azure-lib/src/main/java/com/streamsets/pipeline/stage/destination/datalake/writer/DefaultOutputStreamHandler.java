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

import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;
import org.apache.commons.io.output.CountingOutputStream;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

final class DefaultOutputStreamHandler implements OutputStreamHelper {
  private final static String DOT = ".";

  private final ADLStoreClient client;
  private final String uniquePrefix;
  private final String fileNameSuffix;
  private final Map<String, Long> filePathCount;
  private final long maxRecordsPerFile;
  private final long maxFileSize;
  private final String tempFileName;
  private final String uniqueId;
  private CountingOutputStream countingOutputStream;
  private final ConcurrentLinkedQueue<String> closedPaths;

  public DefaultOutputStreamHandler(
      ADLStoreClient client,
      String uniquePrefix,
      String fileNameSuffix,
      String uniqueId,
      long maxRecordsPerFile,
      long maxFileSize,
      ConcurrentLinkedQueue<String> closedPaths
  ) {
    filePathCount = new HashMap<>();
    this.client = client;
    this.uniquePrefix = uniquePrefix;
    this.fileNameSuffix = fileNameSuffix;
    this.uniqueId = uniqueId.replaceAll(":", "-");
    this.maxRecordsPerFile = maxRecordsPerFile;
    this.maxFileSize = maxFileSize;
    this.tempFileName = TMP_FILE_PREFIX + uniquePrefix + "-" + this.uniqueId + getExtention();
    this.closedPaths = closedPaths;
  }

  @Override
  public CountingOutputStream getOutputStream(String filePath)
      throws StageException, IOException {
    ADLFileOutputStream stream;
    if (!client.checkExists(filePath)) {
      stream = client.createFile(filePath, IfExists.FAIL);
    } else {
      stream = client.getAppendStream(filePath);
    }

    countingOutputStream = new CountingOutputStream(stream);

    return countingOutputStream;
  }

  @Override
  public void commitFile(String dirPath) throws IOException {
    String filePath = dirPath + "/" +
        tempFileName.replaceFirst(TMP_FILE_PREFIX + uniquePrefix + "-" + uniqueId, uniquePrefix + "-" + UUID.randomUUID());
    client.rename(dirPath + "/" + tempFileName, filePath);
    closedPaths.add(filePath);
  }

  @Override
  public String getTempFilePath(String dirPath, Record record, Date recordTime) throws ELEvalException {
    return dirPath + "/" + tempFileName;
  }

  @Override
  public void clearStatus() throws IOException {
    for (String dirPath : filePathCount.keySet()) {
      commitFile(dirPath);
    }
    filePathCount.clear();
  }

  @Override
  public boolean shouldRoll(String dirPath) {
    if (maxRecordsPerFile <= 0 && maxFileSize <= 0) {
      return false;
    }

    Long count = filePathCount.get(dirPath);
    long size = countingOutputStream.getByteCount();

    if (count == null) {
      count = 1L;
    }

    if (maxRecordsPerFile > 0 && count >= maxRecordsPerFile) {
      filePathCount.put(dirPath, 1L);
      return true;
    }

    if (maxFileSize > 0 && size >= maxFileSize) {
      filePathCount.put(dirPath, 1L);
      return true;
    }

    count++;

    filePathCount.put(dirPath, count);
    return false;
  }

  @Override
  public StreamCloseEventHandler<?> getStreamCloseEventHandler() {
    return null;
  }

  private String getExtention() {
    StringBuilder extension = new StringBuilder();

    if (fileNameSuffix != null && !fileNameSuffix.isEmpty()) {
      extension.append(DOT);
      extension = extension.append(fileNameSuffix);
    }

    return extension.toString();
  }
}
