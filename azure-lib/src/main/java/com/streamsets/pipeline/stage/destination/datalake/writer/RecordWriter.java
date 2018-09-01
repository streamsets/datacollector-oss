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

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.ContentSummary;
import com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.stage.destination.datalake.DataLakeTarget;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RecordWriter {
  private final static Logger LOG = LoggerFactory.getLogger(RecordWriter.class);

  // FilePath with ADLS connections stream
  private Map<String, DataLakeDataGenerator> generators;
  private final ADLStoreClient client;
  private final DataFormat dataFormat;
  private final DataGeneratorFormatConfig dataFormatConfig;
  private final String uniquePrefix;
  private final String fileNameSuffix;
  private final String fileNameEL;
  private final boolean dirPathTemplateInHeader;
  private final Target.Context context;

  private final ELEval dirPathTemplateEval;
  private final ELVars dirPathTemplateVars;
  private final ELEval fileNameEval;
  private final ELVars fileNameVars;
  private final boolean rollIfHeader;
  private final String rollHeaderName;
  private final long maxRecordsPerFile;
  private final long maxFileSize;
  private final WholeFileExistsAction wholeFileExistsAction;
  private final OutputStreamHelper outputStreamHelper;
  private final String authTokenEndpoint;
  private final String clientId;
  private final String clientKey;
  private final long idleTimeSecs;

  private final ConcurrentLinkedQueue<String> closedPaths;

  public RecordWriter(
      ADLStoreClient client,
      DataFormat dataFormat,
      DataGeneratorFormatConfig dataFormatConfig,
      String uniquePrefix,
      String fileNameSuffix,
      String fileNameEL,
      boolean dirPathTemplateInHeader,
      Target.Context context,
      boolean rollIfHeader,
      String rollHeaderName,
      long maxRecordsPerFile,
      long maxFileSize,
      WholeFileExistsAction wholeFileExistsAction,
      String authTokenEndpoint,
      String clientId,
      String clientKey,
      long idleTimeSecs
  ) {
    generators = new HashMap<>();
    dirPathTemplateEval = context.createELEval("dirPathTemplate");
    dirPathTemplateVars = context.createELVars();
    fileNameEval = context.createELEval("fileNameEL");
    fileNameVars = context.createELVars();
    closedPaths = new ConcurrentLinkedQueue<>();

    this.client = client;
    this.dataFormat = dataFormat;
    this.dataFormatConfig = dataFormatConfig;
    this.uniquePrefix = uniquePrefix;
    this.fileNameSuffix = fileNameSuffix;
    this.fileNameEL = fileNameEL;
    this.dirPathTemplateInHeader = dirPathTemplateInHeader;
    this.context = context;
    this.rollIfHeader = rollIfHeader;
    this.rollHeaderName = rollHeaderName;
    this.maxRecordsPerFile = maxRecordsPerFile;
    this.maxFileSize = maxFileSize;
    this.wholeFileExistsAction = wholeFileExistsAction;
    this.authTokenEndpoint = authTokenEndpoint;
    this.clientId = clientId;
    this.clientKey = clientKey;
    this.idleTimeSecs = idleTimeSecs;

    this.outputStreamHelper = getOutputStreamHelper();
  }

  public void updateToken() throws IOException {
    AzureADToken token = AzureADAuthenticator.getTokenUsingClientCreds(authTokenEndpoint, clientId, clientKey);
    client.updateToken(token);
  }

  public void write(String filePath, Record record) throws StageException, IOException {
    DataLakeDataGenerator generator = getGenerator(filePath);
    generator.write(record);
  }

  /*
  return the full filePath for a record
   */
  public String getFilePath(
      String dirPathTemplate,
      Record record,
      Date recordTime
  ) throws StageException {
    String dirPath;
    // get directory path
    if (dirPathTemplateInHeader) {
      dirPath = record.getHeader().getAttribute(DataLakeTarget.TARGET_DIRECTORY_HEADER);

      Utils.checkArgument(!(dirPath == null || dirPath.isEmpty()), "Directory Path cannot be null");
    } else {
      dirPath = resolvePath(dirPathTemplateEval, dirPathTemplateVars, dirPathTemplate, recordTime, record);
    }

    // SDC-5492: replace "//" to "/" in file path
    dirPath = dirPath.replaceAll("/+","/");
    if (dirPath.endsWith("/")) {
      dirPath = dirPath.substring(0, dirPath.length()-1);
    }

    return outputStreamHelper.getTempFilePath(dirPath, record, recordTime);
  }

  public void close() throws IOException, StageException {
    for (Map.Entry<String, DataLakeDataGenerator> entry : generators.entrySet()) {
      String dirPath = entry.getKey().substring(0, entry.getKey().lastIndexOf("/"));
      entry.getValue().close();
      generators.remove(dirPath);
      outputStreamHelper.commitFile(dirPath);
    }
    generators.clear();
    outputStreamHelper.clearStatus();
  }

  public void flush(String filePath) throws IOException {
    DataLakeDataGenerator generator = generators.get(filePath);
    if (generator == null) {
      return;
    }
    generator.flush();
  }

  private DataLakeDataGenerator getGenerator(String filePath) throws StageException, IOException {
    DataLakeDataGenerator generator = generators.get(filePath);
    if (generator == null) {
      generator = createDataGenerator(filePath);
      generators.put(filePath, generator);
    }
    return generator;
  }

  private DataLakeDataGenerator createDataGenerator(String filePath) throws StageException, IOException {
    return new DataLakeDataGenerator(filePath, outputStreamHelper, dataFormatConfig, idleTimeSecs);
  }

  OutputStreamHelper getOutputStreamHelper() {
    final String uniqueId = context.getSdcId() + "-" + context.getPipelineId() + "-" + context.getRunnerId();

    if (dataFormat != DataFormat.WHOLE_FILE) {
      return new DefaultOutputStreamHandler(
          client,
          uniquePrefix,
          fileNameSuffix,
          uniqueId,
          maxRecordsPerFile,
          maxFileSize,
          closedPaths
      );
    } else {
      return new WholeFileFormatOutputStreamHandler(
          context,
          client,
          uniquePrefix,
          fileNameEL,
          fileNameEval,
          fileNameVars,
          wholeFileExistsAction
      );
    }
  }

  private String resolvePath(
      ELEval dirPathTemplateEval,
      ELVars dirPathTemplateVars,
      String dirPathTemplate,
      Date date,
      Record record
  ) throws ELEvalException {
    RecordEL.setRecordInContext(dirPathTemplateVars, record);
    if (date != null) {
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      TimeEL.setCalendarInContext(dirPathTemplateVars, calendar);
    }
    return dirPathTemplateEval.eval(dirPathTemplateVars, dirPathTemplate, String.class);
  }

  @VisibleForTesting
  boolean shouldRoll(Record record, String dirPath) {
    if (rollIfHeader && record.getHeader().getAttribute(rollHeaderName) != null) {
      return true;
    }

    return outputStreamHelper.shouldRoll(dirPath);
  }

  /**
   * Produce events that were cached during the batch processing.
   */
  public void issueCachedEvents() throws IOException {
    String closedPath;
    while((closedPath = closedPaths.poll()) != null) {
      produceCloseFileEvent(closedPath);
    }
  }

  private void produceCloseFileEvent(String finalPath) throws IOException {
    ContentSummary summary = client.getContentSummary(finalPath);
    DataLakeEvents.CLOSED_FILE.create(context)
        .with("filepath", finalPath)
        .with("filename", finalPath.substring(finalPath.lastIndexOf("/")+1))
        .with("length", summary.length)
        .createAndSend();
  }
}
