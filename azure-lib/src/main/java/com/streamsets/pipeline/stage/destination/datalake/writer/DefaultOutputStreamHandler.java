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

import com.google.common.collect.ImmutableSet;
import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;
import com.streamsets.pipeline.stage.destination.datalake.Errors;
import org.apache.commons.io.output.CountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

final class DefaultOutputStreamHandler implements OutputStreamHelper {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultOutputStreamHandler.class);
  private static final String DOT = ".";

  private final ADLStoreClient client;
  private final String uniquePrefix;
  private final String fileNameSuffix;
  private final Map<String, Long> filePathCount;
  private final long maxRecordsPerFile;
  private final long maxFileSize;
  private final String tempFileName;
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
    this.maxRecordsPerFile = maxRecordsPerFile;
    this.maxFileSize = maxFileSize;
    this.tempFileName = TMP_FILE_PREFIX + uniquePrefix + "-" +
        uniqueId.replaceAll(":", "-") + getExtention();
    this.closedPaths = closedPaths;
  }

  @Override
  public CountingOutputStream getOutputStream(String filePath)
      throws StageException, IOException {
    ADLFileOutputStream stream;
    // we should open the new file, never append to existing file
    stream = client.createFile(filePath, IfExists.FAIL);

    countingOutputStream = new CountingOutputStream(stream);

    return countingOutputStream;
  }

  @Override
  public void commitFile(String dirPath) throws IOException {
    String filePath = dirPath + "/" + uniquePrefix + "-" + UUID.randomUUID() + getExtention();
    String tmpFileToRename = dirPath + "/" + tempFileName;
    boolean renamed = client.rename(tmpFileToRename, filePath);
    if (!renamed) {
      String errorMessage = Utils.format("Failed to rename file '{}' to '{}'", tmpFileToRename, filePath);
      LOG.error(errorMessage);
      throw new IOException(tmpFileToRename);
    }
    //Once committed remove the dirPath from filePathCount
    filePathCount.remove(dirPath);
    closedPaths.add(filePath);
  }

  @Override
  public String getTempFilePath(String dirPath, Record record, Date recordTime) throws ELEvalException {
    return dirPath + "/" + tempFileName;
  }

  @Override
  public void clearStatus() throws IOException {
    Set<String> dirPathsToCommit = ImmutableSet.copyOf(filePathCount.keySet());
    for (String dirPath : dirPathsToCommit) {
      commitFile(dirPath);
    }
  }

  @Override
  public boolean shouldRoll(String dirPath) {
    if (maxRecordsPerFile <= 0 && maxFileSize <= 0) {
      return false;
    }

    if (countingOutputStream == null) { //We don't have an open file, no need to roll
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
