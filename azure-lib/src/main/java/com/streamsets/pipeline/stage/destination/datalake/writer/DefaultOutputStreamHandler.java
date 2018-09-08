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

import com.google.common.base.Strings;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

final class DefaultOutputStreamHandler implements OutputStreamHelper {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultOutputStreamHandler.class);
  private static final String DOT = ".";

  private final ADLStoreClient client;
  private final String uniquePrefix;
  private final String fileNameSuffix;
  private final long maxRecordsPerFile;
  private final long maxFileSize;
  private final String tempFileName;
  private final ConcurrentLinkedQueue<String> closedPaths;

  DefaultOutputStreamHandler(
      ADLStoreClient client,
      String uniquePrefix,
      String fileNameSuffix,
      String uniqueId,
      long maxRecordsPerFile,
      long maxFileSize,
      ConcurrentLinkedQueue<String> closedPaths
  ) {
    this.client = client;
    this.uniquePrefix = uniquePrefix;
    this.fileNameSuffix = fileNameSuffix;
    this.maxRecordsPerFile = maxRecordsPerFile;
    this.maxFileSize = maxFileSize;
    this.tempFileName = TMP_FILE_PREFIX + uniquePrefix + "-" +
        uniqueId.replaceAll(":", "-") + getExtension();
    this.closedPaths = closedPaths;
  }

  @Override
  public OutputStream getOutputStream(String filePath) throws IOException {
    // we should open the new file, never append to existing file
    return client.createFile(filePath, IfExists.FAIL);
  }

  @Override
  public void commitFile(String tmpFilePath) throws IOException {
    String dirPath = getDirPathForFile(tmpFilePath);
    String filePath = dirPath + "/" + uniquePrefix + "-" + UUID.randomUUID() + getExtension();
    String tmpFileToRename = dirPath + "/" + tempFileName;
    LOG.debug("Renaming {} to {}", tmpFileToRename, filePath);
    boolean renamed = client.rename(tmpFileToRename, filePath);
    if (!renamed) {
      String errorMessage = Utils.format("Failed to rename file '{}' to '{}'", tmpFileToRename, filePath);
      LOG.error(errorMessage);
      throw new IOException(tmpFileToRename);
    }
    closedPaths.add(filePath);
  }

  @Override
  public String getTempFilePath(String dirPath, Record record, Date recordTime) throws ELEvalException {
    return dirPath + "/" + tempFileName;
  }

  @Override
  public boolean shouldRoll(DataLakeDataGenerator dataGenerator) {
    if (maxRecordsPerFile <= 0 && maxFileSize <= 0) {
      return false;
    }

    String tmpFilePath = dataGenerator.getFilePath() + '/' + tempFileName;

    if (maxRecordsPerFile > 0 && dataGenerator.getRecordCount() >= maxRecordsPerFile) {
      LOG.debug(
          "Max Records per file reached, Num of records {} in the file path {}",
          dataGenerator.getRecordCount(),
          tmpFilePath
      );
      return true;
    }

    if (maxFileSize > 0 && dataGenerator.getByteCount() >= maxFileSize) {
      LOG.debug("Max File Size reached, Size {} of the file path {}", dataGenerator.getByteCount(), tmpFilePath);
      return true;
    }

    return false;
  }

  @Override
  public StreamCloseEventHandler<?> getStreamCloseEventHandler() {
    return null;
  }

  private String getExtension() {
    StringBuilder extension = new StringBuilder();
    if (!Strings.isNullOrEmpty(fileNameSuffix)) {
      extension.append(DOT).append(fileNameSuffix);
    }
    return extension.toString();
  }
}
